"""Stage 2a - Kafka Producer.

Reads the preserved API records (data/raw_data.json), wraps each record in a
JSON envelope and publishes it to the Kafka topic. After all records it sends
one END control message that tells the consumer how many records belong to
this run (the consumer's termination condition).

Run standalone:  python -m src.producer
"""

import json
import logging
import sys
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src import config
from src.api_client import load_flat_records_from_raw
from src.metrics import update_metrics

logger = logging.getLogger(__name__)


def check_broker(admin: AdminClient, timeout: float = 10.0) -> None:
    """Fail fast with a clear message if the broker is not reachable."""
    try:
        metadata = admin.list_topics(timeout=timeout)
    except KafkaException as exc:
        raise ConnectionError(
            f"Kafka broker at {config.KAFKA_BOOTSTRAP_SERVERS} is not reachable. "
            "Start it with: docker compose up -d"
        ) from exc
    logger.info("Connected to Kafka cluster with %s broker(s).", len(metadata.brokers))


def ensure_topic(admin: AdminClient) -> None:
    """Create the topic if it does not exist (safe to call repeatedly)."""
    existing = admin.list_topics(timeout=10).topics
    if config.KAFKA_TOPIC in existing:
        print(f"[PRODUCER] Topic '{config.KAFKA_TOPIC}' already exists.")
        return
    new_topic = NewTopic(
        config.KAFKA_TOPIC,
        num_partitions=config.KAFKA_TOPIC_PARTITIONS,
        replication_factor=config.KAFKA_REPLICATION_FACTOR,
    )
    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[PRODUCER] Topic '{topic}' created "
                  f"(partitions={config.KAFKA_TOPIC_PARTITIONS}, "
                  f"replication={config.KAFKA_REPLICATION_FACTOR}).")
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[PRODUCER] Topic '{topic}' already exists.")
            else:
                raise


def serialize(message: dict) -> bytes:
    """JSON serialization: Python dict -> UTF-8 encoded JSON bytes."""
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


def run_producer() -> dict:
    records = load_flat_records_from_raw()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]

    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    check_broker(admin)
    ensure_topic(admin)

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",                 # wait for full acknowledgement
        "enable.idempotence": True,    # no duplicates caused by producer retries
        "retries": 5,
        "linger.ms": 20,
        "client.id": "openfda-producer",
    })

    stats = {"delivered": 0, "failed": 0, "serialization_errors": 0}

    def on_delivery(err, msg) -> None:
        # Called once per message when the broker confirms (or rejects) it.
        if err is not None:
            stats["failed"] += 1
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        elif msg.key() != b"__END__":
            stats["delivered"] += 1

    attempted = 0
    for sequence, record in enumerate(records, start=1):
        envelope = {
            "message_type": "RECORD",
            "run_id": run_id,
            "sequence": sequence,
            "produced_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "payload": record,
        }
        try:
            value = serialize(envelope)
        except (TypeError, ValueError) as exc:
            stats["serialization_errors"] += 1
            logger.error("Could not serialize record %s: %s", record.get("safety_report_id"), exc)
            continue

        key = str(record.get("safety_report_id") or f"no-id-{sequence}").encode("utf-8")
        while True:
            try:
                producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=on_delivery)
                attempted += 1
                break
            except BufferError:
                # Local queue is full: serve delivery callbacks, then retry.
                producer.poll(1.0)
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        logger.error("%s message(s) were not delivered before flush timeout.", remaining)

    # Control message: lets the consumer know the exact size of this run.
    end_message = {
        "message_type": "END",
        "run_id": run_id,
        "total_published": stats["delivered"],
        "produced_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    producer.produce(config.KAFKA_TOPIC, key=b"__END__", value=serialize(end_message), on_delivery=on_delivery)
    producer.flush(10)

    manifest = {
        "run_id": run_id,
        "topic": config.KAFKA_TOPIC,
        "records_read": len(records),
        "messages_attempted": attempted,
        "messages_published": stats["delivered"],
        "messages_failed": stats["failed"],
        "serialization_errors": stats["serialization_errors"],
    }
    with open(config.PRODUCER_MANIFEST_PATH, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    update_metrics("producer", manifest)

    print(f"[PRODUCER] Run id: {run_id}")
    print(f"[PRODUCER] Records read from raw data : {len(records)}")
    print(f"[PRODUCER] Messages published to Kafka: {stats['delivered']}")
    print(f"[PRODUCER] Delivery failures          : {stats['failed']}")
    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run_producer()
    except (ConnectionError, FileNotFoundError, KafkaException) as error:
        print(f"[PRODUCER] ERROR: {error}")
        sys.exit(1)
