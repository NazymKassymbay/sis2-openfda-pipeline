"""Small helper that records stage-level counts in one JSON file.

Every stage writes its numbers here, so the final pipeline report compares
counts produced by the actual run instead of manually typed values.
"""

import json
from datetime import datetime, timezone
from typing import Any

from src.config import METRICS_PATH, ensure_directories


def load_metrics() -> dict:
    if METRICS_PATH.exists():
        with open(METRICS_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def update_metrics(stage: str, values: dict[str, Any]) -> None:
    """Merge `values` into the metrics file under the `stage` key."""
    ensure_directories()
    metrics = load_metrics()
    entry = metrics.get(stage, {})
    entry.update(values)
    entry["updated_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    metrics[stage] = entry
    with open(METRICS_PATH, "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2, ensure_ascii=False, default=str)
