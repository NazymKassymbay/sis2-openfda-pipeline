"""Stage 2b - Kafka Consumer.

Subscribes to the topic, deserializes JSON messages and keeps the records that
belong to the producer run recorded in data/producer_manifest.json.

Termination condition (the consumer never runs forever):
  1. the END control message of this run was received AND the number of
     consumed records equals the `total_published` value it carries, or
  2. no message arrived for CONSUMER_IDLE_TIMEOUT_SEC seconds.

Run standalone:  python -m src.consumer
"""

import json
import logging
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from src import config
from src.metrics import update_metrics

logger = logging.getLogger(__name__)


def deserialize(raw_value: bytes) -> dict:
    """JSON deserialization: UTF-8 bytes -> Python dict."""
    return json.loads(raw_value.decode("utf-8"))


def read_run_id() -> str:
    if not config.PRODUCER_MANIFEST_PATH.exists():
        raise FileNotFoundError("Producer manifest not found. Run the producer first.")
    with open(config.PRODUCER_MANIFEST_PATH, encoding="utf-8") as fh:
        return json.load(fh)["run_id"]


def run_consumer(run_id: str | None = None) -> list[dict]:
    run_id = run_id or read_run_id()
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        # A new group per run + 'earliest' guarantees the run is read from the start.
        "group.id": f"openfda-consumer-{run_id}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "openfda-consumer",
    })

    records: list[dict] = []
    stats = {"received_total": 0, "other_runs_skipped": 0, "deserialization_errors": 0}
    expected_total: int | None = None
    last_message_time = time.monotonic()
    stop_reason = "idle timeout"

    try:
        consumer.subscribe([config.KAFKA_TOPIC])
        print(f"[CONSUMER] Subscribed to '{config.KAFKA_TOPIC}', waiting for run {run_id} ...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_message_time > config.CONSUMER_IDLE_TIMEOUT_SEC:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            last_message_time = time.monotonic()
            stats["received_total"] += 1
            try:
                message = deserialize(msg.value())
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                stats["deserialization_errors"] += 1
                logger.error("Skipping undecodable message at offset %s: %s", msg.offset(), exc)
                continue

            if message.get("run_id") != run_id:
                stats["other_runs_skipped"] += 1  # messages from earlier runs
                continue

            if message.get("message_type") == "END":
                expected_total = int(message["total_published"])
                print(f"[CONSUMER] END marker received (expected {expected_total} records).")
            elif message.get("message_type") == "RECORD":
                records.append(message["payload"])
                if len(records) % 100 == 0:
                    print(f"[CONSUMER] Consumed {len(records)} records ...")

            if expected_total is not None and len(records) >= expected_total:
                stop_reason = "END marker and all records received"
                break

        if records:
            consumer.commit(asynchronous=False)  # store the group's read position
    finally:
        consumer.close()

    with open(config.CONSUMED_JSON_PATH, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False)

    summary = {
        "run_id": run_id,
        "messages_consumed": len(records),
        "expected_from_end_marker": expected_total,
        "messages_from_other_runs_skipped": stats["other_runs_skipped"],
        "deserialization_errors": stats["deserialization_errors"],
        "stop_reason": stop_reason,
    }
    update_metrics("consumer", summary)
    print(f"[CONSUMER] Messages consumed for this run : {len(records)}")
    print(f"[CONSUMER] Older-run messages skipped      : {stats['other_runs_skipped']}")
    print(f"[CONSUMER] Deserialization errors          : {stats['deserialization_errors']}")
    print(f"[CONSUMER] Stopped because                 : {stop_reason}")
    return records


def load_consumed_records() -> list[dict]:
    with open(config.CONSUMED_JSON_PATH, encoding="utf-8") as fh:
        return json.load(fh)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run_consumer(sys.argv[1] if len(sys.argv) > 1 else None)
    except (FileNotFoundError, KafkaException) as error:
        print(f"[CONSUMER] ERROR: {error}")
        sys.exit(1)
