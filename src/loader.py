"""Stage 5 - Storage in SQLite3 with duplicate prevention (idempotent load).

Tables
  adverse_events : one row per report, PRIMARY KEY safety_report_id
  reactions      : one row per (report, reaction), composite PRIMARY KEY,
                   FOREIGN KEY -> adverse_events
  load_runs      : audit log of every load execution

Idempotency: rows are written with INSERT ... ON CONFLICT DO UPDATE (upsert).
An existing row is only updated when its content really changed, so loading
the same dataset twice inserts nothing and changes nothing.
"""

import hashlib
import sqlite3
from datetime import datetime, timezone

import pandas as pd

from src import config
from src.metrics import update_metrics

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS adverse_events (
    safety_report_id            TEXT    PRIMARY KEY,
    report_version              INTEGER NOT NULL,
    receive_date                TEXT    NOT NULL,           -- ISO date YYYY-MM-DD
    receipt_date                TEXT,
    transmission_date           TEXT,
    receive_month               TEXT    NOT NULL,
    report_type                 TEXT,
    serious_label               TEXT    NOT NULL CHECK (serious_label IN ('Serious', 'Non-serious')),
    is_serious                  INTEGER NOT NULL CHECK (is_serious IN (0, 1)),
    seriousness_death           INTEGER NOT NULL CHECK (seriousness_death IN (0, 1)),
    seriousness_hospitalization INTEGER NOT NULL CHECK (seriousness_hospitalization IN (0, 1)),
    seriousness_life_threatening INTEGER NOT NULL CHECK (seriousness_life_threatening IN (0, 1)),
    seriousness_disabling       INTEGER NOT NULL CHECK (seriousness_disabling IN (0, 1)),
    seriousness_congenital_anomaly INTEGER NOT NULL CHECK (seriousness_congenital_anomaly IN (0, 1)),
    seriousness_other           INTEGER NOT NULL CHECK (seriousness_other IN (0, 1)),
    seriousness_criteria_count  INTEGER NOT NULL,
    occur_country               TEXT,
    primary_source_country      TEXT,
    reporter_type               TEXT,
    patient_sex                 TEXT    NOT NULL CHECK (patient_sex IN ('Male', 'Female', 'Unknown')),
    patient_age                 REAL,
    patient_age_unit            TEXT,
    age_years                   REAL    CHECK (age_years IS NULL OR age_years BETWEEN 0 AND 120),
    age_group                   TEXT    NOT NULL,
    patient_weight_kg           REAL    CHECK (patient_weight_kg IS NULL OR patient_weight_kg > 0),
    num_drugs                   INTEGER NOT NULL CHECK (num_drugs >= 1),
    num_suspect_drugs           INTEGER NOT NULL,
    num_reactions               INTEGER NOT NULL CHECK (num_reactions >= 1),
    is_polypharmacy             INTEGER NOT NULL CHECK (is_polypharmacy IN (0, 1)),
    primary_suspect_drug        TEXT    NOT NULL,
    primary_drug_indication     TEXT,
    primary_drug_route          TEXT,
    days_to_latest_info         INTEGER,
    loaded_at_utc               TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS reactions (
    safety_report_id TEXT NOT NULL REFERENCES adverse_events (safety_report_id) ON DELETE CASCADE,
    reaction_pt      TEXT NOT NULL,
    reaction_outcome TEXT NOT NULL,
    PRIMARY KEY (safety_report_id, reaction_pt)
);

CREATE TABLE IF NOT EXISTS load_runs (
    run_no            INTEGER PRIMARY KEY AUTOINCREMENT,
    executed_at_utc   TEXT    NOT NULL,
    rows_processed    INTEGER NOT NULL,
    rows_before       INTEGER NOT NULL,
    rows_after        INTEGER NOT NULL,
    rows_inserted     INTEGER NOT NULL,
    rows_updated      INTEGER NOT NULL,
    reaction_rows_after INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_drug ON adverse_events (primary_suspect_drug);
CREATE INDEX IF NOT EXISTS idx_events_month ON adverse_events (receive_month);
"""

EVENT_COLUMNS = [
    "safety_report_id", "report_version", "receive_date", "receipt_date", "transmission_date",
    "receive_month", "report_type", "serious_label", "is_serious", "seriousness_death",
    "seriousness_hospitalization", "seriousness_life_threatening", "seriousness_disabling",
    "seriousness_congenital_anomaly", "seriousness_other", "seriousness_criteria_count",
    "occur_country", "primary_source_country", "reporter_type", "patient_sex", "patient_age",
    "patient_age_unit", "age_years", "age_group", "patient_weight_kg", "num_drugs",
    "num_suspect_drugs", "num_reactions", "is_polypharmacy", "primary_suspect_drug",
    "primary_drug_indication", "primary_drug_route", "days_to_latest_info",
]

VERIFICATION_SQL_PATH = config.PROJECT_ROOT / "sql" / "verification_queries.sql"


def read_verification_queries() -> dict[str, str]:
    """Parse sql/verification_queries.sql into {name: query} using '-- name:' markers."""
    queries, name, lines = {}, None, []
    for line in VERIFICATION_SQL_PATH.read_text(encoding="utf-8").splitlines():
        if line.startswith("-- name:"):
            if name:
                queries[name] = "\n".join(lines).strip()
            name, lines = line.split(":", 1)[1].strip(), []
        elif name and not line.startswith("--"):
            lines.append(line)
    if name:
        queries[name] = "\n".join(lines).strip()
    return queries


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def _to_db_value(value):
    """Convert pandas/NumPy values to plain Python types SQLite understands."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item"):  # NumPy scalar -> Python scalar
        return value.item()
    return value


def table_fingerprint(conn: sqlite3.Connection) -> str:
    """SHA-256 of the stored business data (audit timestamp excluded)."""
    digest = hashlib.sha256()
    cols = ", ".join(EVENT_COLUMNS)
    for row in conn.execute(f"SELECT {cols} FROM adverse_events ORDER BY safety_report_id"):
        digest.update(repr(row).encode("utf-8"))
    for row in conn.execute("SELECT * FROM reactions ORDER BY safety_report_id, reaction_pt"):
        digest.update(repr(row).encode("utf-8"))
    return digest.hexdigest()


def load_data(clean_df: pd.DataFrame, reactions: pd.DataFrame) -> dict:
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    event_rows = [tuple(_to_db_value(v) for v in row) + (loaded_at,)
                  for row in clean_df[EVENT_COLUMNS].itertuples(index=False, name=None)]
    reaction_rows = [tuple(_to_db_value(v) for v in row)
                     for row in reactions[["safety_report_id", "reaction_pt", "reaction_outcome"]]
                     .itertuples(index=False, name=None)]

    placeholders = ", ".join(["?"] * (len(EVENT_COLUMNS) + 1))
    update_cols = [c for c in EVENT_COLUMNS if c != "safety_report_id"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    # Update only when content differs and the incoming version is not older.
    changed = " OR ".join(f"adverse_events.{c} IS NOT excluded.{c}" for c in update_cols)
    upsert_events = f"""
        INSERT INTO adverse_events ({", ".join(EVENT_COLUMNS)}, loaded_at_utc)
        VALUES ({placeholders})
        ON CONFLICT (safety_report_id) DO UPDATE SET {set_clause}
        WHERE excluded.report_version >= adverse_events.report_version AND ({changed});
    """
    upsert_reactions = """
        INSERT INTO reactions (safety_report_id, reaction_pt, reaction_outcome) VALUES (?, ?, ?)
        ON CONFLICT (safety_report_id, reaction_pt) DO UPDATE SET reaction_outcome = excluded.reaction_outcome
        WHERE reactions.reaction_outcome IS NOT excluded.reaction_outcome;
    """

    conn = get_connection()
    try:
        create_schema(conn)
        rows_before = conn.execute("SELECT COUNT(*) FROM adverse_events").fetchone()[0]
        with conn:  # one transaction: all rows or nothing
            changes_start = conn.total_changes
            conn.executemany(upsert_events, event_rows)
            event_changes = conn.total_changes - changes_start
            conn.executemany(upsert_reactions, reaction_rows)
        rows_after = conn.execute("SELECT COUNT(*) FROM adverse_events").fetchone()[0]
        reaction_rows_after = conn.execute("SELECT COUNT(*) FROM reactions").fetchone()[0]
        inserted = rows_after - rows_before
        updated = event_changes - inserted
        with conn:
            conn.execute(
                "INSERT INTO load_runs (executed_at_utc, rows_processed, rows_before, rows_after, "
                "rows_inserted, rows_updated, reaction_rows_after) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (loaded_at, len(event_rows), rows_before, rows_after, inserted, updated, reaction_rows_after),
            )
        fingerprint = table_fingerprint(conn)
    except sqlite3.Error as exc:
        raise RuntimeError(f"SQLite load failed: {exc}") from exc
    finally:
        conn.close()

    result = {"rows_processed": len(event_rows), "rows_before": rows_before, "rows_after": rows_after,
              "rows_inserted": inserted, "rows_updated": updated,
              "reaction_rows_after": reaction_rows_after, "fingerprint": fingerprint}
    print(f"[LOAD] processed={len(event_rows)} before={rows_before} after={rows_after} "
          f"inserted={inserted} updated={updated} reactions={reaction_rows_after}")
    return result


def run_verification_queries() -> dict:
    """Execute SQL checks, print them and save them to data/sql_verification.txt."""
    conn = get_connection()
    output_lines, results = [], {}
    try:
        for name, sql in read_verification_queries().items():
            frame = pd.read_sql_query(sql, conn)
            results[name] = frame.to_dict(orient="records")
            output_lines += [f"-- {name}", " ".join(sql.split()), frame.to_string(index=False), ""]
    finally:
        conn.close()
    text = "\n".join(output_lines)
    (config.DATA_DIR / "sql_verification.txt").write_text(text, encoding="utf-8")
    print(text)
    return results


def run_idempotent_load(clean_df: pd.DataFrame, reactions: pd.DataFrame) -> dict:
    """Load twice with the same data and prove nothing is duplicated or changed."""
    print("[LOAD] Execution #1")
    first = load_data(clean_df, reactions)
    print("[LOAD] Execution #2 (same dataset)")
    second = load_data(clean_df, reactions)

    idempotent = (second["rows_inserted"] == 0 and second["rows_updated"] == 0
                  and first["rows_after"] == second["rows_after"]
                  and first["fingerprint"] == second["fingerprint"])
    verification = run_verification_queries()
    stored = verification["total_reports"][0]["total_reports"]

    summary = {
        "records_processed": len(clean_df),
        "records_stored": stored,
        "processed_equals_stored": len(clean_df) == stored,
        "first_load": first,
        "second_load": second,
        "idempotency_passed": idempotent,
        "duplicate_ids_in_db": len(verification["duplicate_ids"]),
        "orphan_reactions": verification["orphan_reactions"][0]["orphan_reactions"],
    }
    update_metrics("database", summary)
    print(f"[LOAD] Processed records: {len(clean_df)} | Stored records: {stored}")
    print(f"[LOAD] Idempotency test passed: {idempotent} "
          f"(fingerprint {first['fingerprint'][:12]} == {second['fingerprint'][:12]})")
    return summary


if __name__ == "__main__":
    clean = pd.read_csv(config.CLEAN_CSV_PATH, dtype={"safety_report_id": str, "patient_age_unit": str},
                        parse_dates=["receive_date", "receipt_date", "transmission_date"])
    reacts = pd.read_csv(config.CLEAN_REACTIONS_PATH, dtype={"safety_report_id": str})
    run_idempotent_load(clean, reacts)
