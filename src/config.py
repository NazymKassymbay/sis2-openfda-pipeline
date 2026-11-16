"""Central configuration for the openFDA -> Kafka -> SQLite pipeline.

All values can be overridden through environment variables or a local `.env`
file (see `.env.example`). Paths are resolved relative to the project root so
the code never depends on a personal machine layout.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional at runtime
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# openFDA API
# ---------------------------------------------------------------------------
OPENFDA_BASE_URL = "https://api.fda.gov/drug/event.json"
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "").strip()  # optional
# Reports received by FDA in this date window are requested (YYYYMMDD).
OPENFDA_DATE_FROM = os.getenv("OPENFDA_DATE_FROM", "20240101")
OPENFDA_DATE_TO = os.getenv("OPENFDA_DATE_TO", "20241231")
# Days of every month that are sampled (one request window per day).
SAMPLE_DAYS = tuple(int(d) for d in os.getenv("SAMPLE_DAYS", "1,7,13,19,25").split(","))
TARGET_RECORDS = int(os.getenv("TARGET_RECORDS", "600"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))  # openFDA max limit is 1000
REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 4

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "drug-adverse-events")
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
# Consumer stops when it receives the END control message for its run,
# or when no message arrives for this many seconds (safety net).
CONSUMER_IDLE_TIMEOUT_SEC = int(os.getenv("CONSUMER_IDLE_TIMEOUT_SEC", "20"))

# ---------------------------------------------------------------------------
# File locations
# ---------------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
FIGURES_DIR = PROJECT_ROOT / "figures"

RAW_JSON_PATH = DATA_DIR / "raw_data.json"          # untouched API responses
RAW_CSV_PATH = DATA_DIR / "raw_data.csv"            # flattened, uncleaned
PRODUCER_MANIFEST_PATH = DATA_DIR / "producer_manifest.json"
CONSUMED_JSON_PATH = DATA_DIR / "consumed_messages.json"
VALID_RECORDS_PATH = DATA_DIR / "validated_records.csv"
INVALID_RECORDS_PATH = DATA_DIR / "invalid_records.csv"
VALIDATION_ERRORS_PATH = DATA_DIR / "validation_errors.csv"
CLEAN_CSV_PATH = DATA_DIR / "clean_data.csv"
CLEAN_REACTIONS_PATH = DATA_DIR / "clean_reactions.csv"
DB_PATH = DATA_DIR / "output.db"
METRICS_PATH = DATA_DIR / "pipeline_metrics.json"


def ensure_directories() -> None:
    """Create output folders if they do not exist yet."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
