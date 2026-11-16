"""Stage 1 - Data collection from the openFDA Drug Adverse Event API.

Endpoint : https://api.fda.gov/drug/event.json
Method   : GET
Params   : search = receivedate:[DAY TO DAY]  (reports received on one day)
           limit  = page size (<= 1000)
           skip   = offset for pagination (<= 25000)
           api_key (optional, raises the daily quota)

Collection procedure: openFDA returns reports of a window in received-date
order, so one big window would only give the first day. Therefore every month
of OPENFDA_DATE_FROM..OPENFDA_DATE_TO is sampled on fixed days (SAMPLE_DAYS,
default 1, 7, 13, 19, 25) and the same number of reports is requested for each
sampled day. The sample is spread over the whole year and is reproducible.

The raw JSON returned by the API is stored unchanged in data/raw_data.json.
A flattened (one row per report) but *uncleaned* copy is stored in
data/raw_data.csv. Both files are the preserved original data.
"""

import json
import logging
import time
from typing import Any

import pandas as pd
import requests

from src import config
from src.metrics import update_metrics

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class OpenFDAError(RuntimeError):
    """Raised when the API cannot return usable data."""


def build_params(date_from: str, date_to: str, skip: int, limit: int) -> dict[str, Any]:
    params = {
        "search": f"receivedate:[{date_from} TO {date_to}]",
        "limit": limit,
        "skip": skip,
    }
    if config.OPENFDA_API_KEY:
        params["api_key"] = config.OPENFDA_API_KEY
    return params


def fetch_page(session: requests.Session, date_from: str, date_to: str, skip: int, limit: int) -> dict:
    """Request one page, retrying on network errors and retryable HTTP codes."""
    params = build_params(date_from, date_to, skip, limit)
    last_error: Exception | None = None

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = session.get(
                config.OPENFDA_BASE_URL, params=params, timeout=config.REQUEST_TIMEOUT_SEC
            )
            if response.status_code == 200:
                return response.json()
            if response.status_code == 404:
                # openFDA answers 404 "No matches found" when skip passes the end.
                logger.warning("No more matches at skip=%s (HTTP 404).", skip)
                return {"results": []}
            if response.status_code in RETRYABLE_STATUS:
                wait = 2 ** attempt
                logger.warning(
                    "HTTP %s on skip=%s (attempt %s/%s). Retrying in %ss.",
                    response.status_code, skip, attempt, config.MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            # Other 4xx errors are not fixed by retrying (bad query, bad key).
            raise OpenFDAError(f"HTTP {response.status_code}: {response.text[:300]}")
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = exc
            wait = 2 ** attempt
            logger.warning("Network error on skip=%s: %s. Retrying in %ss.", skip, exc, wait)
            time.sleep(wait)
        except ValueError as exc:  # invalid JSON body
            raise OpenFDAError(f"Response is not valid JSON: {exc}") from exc

    raise OpenFDAError(f"Failed after {config.MAX_RETRIES} attempts: {last_error}")


def sampling_windows(date_from: str, date_to: str) -> list[tuple[str, str]]:
    """One-day (from, to) windows: the SAMPLE_DAYS of every month in the period."""
    start, end = pd.Timestamp(date_from), pd.Timestamp(date_to)
    windows = []
    for month_start in pd.date_range(start.replace(day=1), end, freq="MS"):
        for day in config.SAMPLE_DAYS:
            sample_day = month_start.replace(day=day)
            if start <= sample_day <= end:
                windows.append((sample_day.strftime("%Y%m%d"), sample_day.strftime("%Y%m%d")))
    return windows


def collect_reports(target: int = config.TARGET_RECORDS) -> list[dict]:
    """Collect about `target` raw reports, spread evenly over the sampled days."""
    windows = sampling_windows(config.OPENFDA_DATE_FROM, config.OPENFDA_DATE_TO)
    per_window = -(-target // len(windows))  # ceiling division
    reports: list[dict] = []
    failed_pages = 0
    requests_sent = 0
    with requests.Session() as session:
        session.headers.update({"User-Agent": "SIS2-DataPipeline/1.0"})
        for date_from, date_to in windows:
            collected_in_window, skip = 0, 0
            while collected_in_window < per_window:
                limit = min(config.PAGE_SIZE, per_window - collected_in_window)
                try:
                    requests_sent += 1
                    payload = fetch_page(session, date_from, date_to, skip, limit)
                except OpenFDAError as exc:
                    failed_pages += 1
                    logger.error("Day %s skip=%s failed: %s", date_from, skip, exc)
                    break
                page = payload.get("results", [])
                if not page:
                    break
                reports.extend(page)
                collected_in_window += len(page)
                skip += len(page)
                time.sleep(0.3)  # stay well below the 240 requests/minute limit
            logger.info("Day %s: %s reports (total %s).", date_from, collected_in_window, len(reports))
            if failed_pages >= 3:
                logger.error("Too many failed requests, stopping collection early.")
                break

    if not reports:
        raise OpenFDAError("No records were collected from openFDA.")
    update_metrics("collection", {
        "api_records_collected": len(reports),
        "day_windows_requested": len(windows),
        "sample_days_per_month": list(config.SAMPLE_DAYS),
        "records_per_day_requested": per_window,
        "http_requests_sent": requests_sent,
        "failed_requests": failed_pages,
        "date_from": config.OPENFDA_DATE_FROM,
        "date_to": config.OPENFDA_DATE_TO,
    })
    return reports


def _first(values: Any) -> Any:
    """Return the first element of a list-like openFDA field, else None."""
    if isinstance(values, list) and values:
        return values[0]
    return None


def flatten_report(report: dict) -> dict:
    """Convert one nested openFDA report into a flat dictionary.

    Values are kept exactly as the API returned them (mostly strings);
    type conversion happens later in the validation/cleaning stages.
    """
    patient = report.get("patient") or {}
    drugs = patient.get("drug") or []
    reactions = patient.get("reaction") or []
    primary_source = report.get("primarysource") or {}

    suspect_drugs = [d for d in drugs if d.get("drugcharacterization") == "1"]
    main_drug = suspect_drugs[0] if suspect_drugs else (drugs[0] if drugs else {})
    main_openfda = main_drug.get("openfda") or {}

    return {
        "safety_report_id": report.get("safetyreportid"),
        "report_version": report.get("safetyreportversion"),
        "receive_date": report.get("receivedate"),
        "receipt_date": report.get("receiptdate"),
        "transmission_date": report.get("transmissiondate"),
        "report_type": report.get("reporttype"),
        "serious": report.get("serious"),
        "seriousness_death": report.get("seriousnessdeath"),
        "seriousness_hospitalization": report.get("seriousnesshospitalization"),
        "seriousness_life_threatening": report.get("seriousnesslifethreatening"),
        "seriousness_disabling": report.get("seriousnessdisabling"),
        "seriousness_congenital_anomaly": report.get("seriousnesscongenitalanomali"),
        "seriousness_other": report.get("seriousnessother"),
        "occur_country": report.get("occurcountry"),
        "primary_source_country": report.get("primarysourcecountry"),
        "reporter_qualification": primary_source.get("qualification"),
        "patient_age": patient.get("patientonsetage"),
        "patient_age_unit": patient.get("patientonsetageunit"),
        "patient_sex": patient.get("patientsex"),
        "patient_weight_kg": patient.get("patientweight"),
        "num_drugs": len(drugs),
        "num_suspect_drugs": len(suspect_drugs),
        "primary_suspect_drug": _first(main_openfda.get("generic_name")) or main_drug.get("medicinalproduct"),
        "primary_drug_indication": main_drug.get("drugindication"),
        "primary_drug_route": _first(main_openfda.get("route")),
        "num_reactions": len(reactions),
        "reactions": [
            {
                "reaction_pt": r.get("reactionmeddrapt"),
                "reaction_outcome": r.get("reactionoutcome"),
            }
            for r in reactions
        ],
    }


def save_raw_data(reports: list[dict], flat_records: list[dict]) -> None:
    """Preserve the original data before any cleaning."""
    config.ensure_directories()
    with open(config.RAW_JSON_PATH, "w", encoding="utf-8") as fh:
        json.dump(reports, fh, ensure_ascii=False)
    raw_df = pd.DataFrame(flat_records)
    raw_df["reactions"] = raw_df["reactions"].apply(lambda v: json.dumps(v, ensure_ascii=False))
    raw_df.to_csv(config.RAW_CSV_PATH, index=False)


def run_collection(target: int = config.TARGET_RECORDS) -> list[dict]:
    reports = collect_reports(target)
    flat_records = [flatten_report(r) for r in reports]
    save_raw_data(reports, flat_records)
    print(f"[COLLECT] API records collected: {len(reports)}")
    print(f"[COLLECT] Raw JSON saved -> {config.RAW_JSON_PATH.relative_to(config.PROJECT_ROOT)}")
    print(f"[COLLECT] Raw CSV  saved -> {config.RAW_CSV_PATH.relative_to(config.PROJECT_ROOT)}")
    return flat_records


def load_flat_records_from_raw() -> list[dict]:
    """Re-create flat records from the preserved raw JSON (no new API call)."""
    if not config.RAW_JSON_PATH.exists():
        raise FileNotFoundError(f"{config.RAW_JSON_PATH} not found. Run the collection stage first.")
    with open(config.RAW_JSON_PATH, encoding="utf-8") as fh:
        reports = json.load(fh)
    return [flatten_report(r) for r in reports]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_collection()
