"""Pipeline orchestrator: openFDA API -> Kafka -> Pandera -> pandas/NumPy -> SQLite -> EDA.

Usage
  python main.py                    # full pipeline (new API collection)
  python main.py --skip-collection  # reuse data/raw_data.json, run the rest

Each stage passes its real output to the next one; nothing is prepared by hand.
"""

import argparse
import logging
import sys
import time

from confluent_kafka import KafkaException

from src import config
from src.analysis import run_analysis
from src.api_client import OpenFDAError, run_collection
from src.cleaner import run_cleaning
from src.consumer import run_consumer
from src.loader import run_idempotent_load
from src.metrics import load_metrics, update_metrics
from src.producer import run_producer
from src.validator import run_validation


def banner(title: str) -> None:
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


def print_final_report() -> dict:
    m = load_metrics()
    collected = m.get("collection", {}).get("api_records_collected")
    published = m["producer"]["messages_published"]
    consumed = m["consumer"]["messages_consumed"]
    val = m["validation"]
    clean = m["cleaning"]
    db = m["database"]

    rows = [
        ("API records collected", collected),
        ("Messages published to Kafka", published),
        ("Messages consumed from Kafka", consumed),
        ("Valid records (Pandera)", val["valid_real_records"]),
        ("Invalid real records (quarantined)", val["invalid_real_records"]),
        ("Deliberately invalid test records detected", f"{val['invalid_test_records_detected']}/{val['test_records_added']}"),
        ("Records after cleaning", clean["rows_out"]),
        ("Records stored in SQLite", db["records_stored"]),
    ]
    banner("PIPELINE VERIFICATION REPORT")
    for label, value in rows:
        print(f"  {label:<45} {value}")

    explanations = []
    if collected is not None and published != collected:
        explanations.append(f"{collected - published} record(s) failed to publish (see producer log).")
    if consumed != published:
        explanations.append(f"{published - consumed} published message(s) were not consumed.")
    if val["invalid_real_records"]:
        explanations.append(f"{val['invalid_real_records']} real record(s) broke validation rules and were "
                            "quarantined in data/invalid_records.csv.")
    if clean["duplicate_ids_found"]:
        explanations.append(f"{clean['duplicate_ids_found']} duplicate report id(s) were removed during cleaning.")
    if not explanations:
        explanations.append("All counts match: no record was lost between stages.")
    print("\n  Differences explained:")
    for line in explanations:
        print(f"   - {line}")

    checks = {
        "published_equals_consumed": published == consumed,
        "consumed_equals_valid_plus_invalid": consumed == val["valid_real_records"] + val["invalid_real_records"],
        "all_test_records_rejected": val["invalid_test_records_detected"] == val["test_records_added"],
        "cleaned_equals_stored": clean["rows_out"] == db["records_stored"],
        "idempotent_reload": db["idempotency_passed"],
        "no_duplicate_ids_in_db": db["duplicate_ids_in_db"] == 0,
        "no_orphan_reactions": db["orphan_reactions"] == 0,
        "at_least_200_messages": consumed >= 200,
    }
    print("\n  Consistency checks:")
    for name, ok in checks.items():
        print(f"   [{'PASS' if ok else 'FAIL'}] {name}")
    update_metrics("verification", {"checks": checks, "explanations": explanations})
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="openFDA adverse-event streaming pipeline")
    parser.add_argument("--skip-collection", action="store_true",
                        help="reuse data/raw_data.json instead of calling the API again")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config.ensure_directories()
    if config.METRICS_PATH.exists():
        previous = load_metrics()
        config.METRICS_PATH.unlink()  # metrics describe the current run only
        if args.skip_collection and "collection" in previous:
            update_metrics("collection", previous["collection"])
    started = time.time()

    try:
        banner("STAGE 1 - API DATA COLLECTION (openFDA)")
        if args.skip_collection:
            if not config.RAW_JSON_PATH.exists():
                raise FileNotFoundError("data/raw_data.json missing - run without --skip-collection first.")
            print("[COLLECT] Skipped: reusing preserved raw data in data/raw_data.json")
        else:
            run_collection()

        banner("STAGE 2 - KAFKA PRODUCER")
        run_producer()

        banner("STAGE 3 - KAFKA CONSUMER")
        records = run_consumer()

        banner("STAGE 4 - PANDERA VALIDATION")
        valid_df = run_validation(records)

        banner("STAGE 5 - CLEANING AND TRANSFORMATION")
        clean_df, reactions = run_cleaning(valid_df)

        banner("STAGE 6 - SQLITE LOAD (executed twice for idempotency)")
        run_idempotent_load(clean_df, reactions)

        banner("STAGE 7 - EDA AND VISUALIZATION")
        run_analysis()

        checks = print_final_report()
    except (OpenFDAError, ConnectionError, KafkaException, FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"\n[PIPELINE] FAILED: {exc}")
        return 1

    print(f"\n[PIPELINE] Finished in {time.time() - started:.1f}s. Metrics -> data/pipeline_metrics.json")
    return 0 if all(checks.values()) else 2


if __name__ == "__main__":
    sys.exit(main())
