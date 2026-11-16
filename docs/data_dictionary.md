# Data Dictionary

Source: openFDA Drug Adverse Event endpoint (`https://api.fda.gov/drug/event.json`), FAERS reports.
Official field reference: https://open.fda.gov/apis/drug/event/searchable-fields/

Type legend: **N** numerical, **C** categorical, **D** date/time, **T** free text, **ID** identifier.

## 1. Raw flattened record (`data/raw_data.csv`, Kafka message payload)

The values are unchanged from the API, so most fields are text strings.

| Column | openFDA field | Kind | Description / allowed values |
|---|---|---|---|
| safety_report_id | `safetyreportid` | ID | Unique report number, e.g. `24012345` or `5801206-7` |
| report_version | `safetyreportversion` | N | Version of the report (higher = newer) |
| receive_date | `receivedate` | D | Date FDA first received the report (YYYYMMDD) |
| receipt_date | `receiptdate` | D | Date of the most recent information (YYYYMMDD) |
| transmission_date | `transmissiondate` | D | Date the record was transmitted (YYYYMMDD) |
| report_type | `reporttype` | C | 1 Spontaneous, 2 Study, 3 Other, 4 Not available to sender |
| serious | `serious` | C | 1 = serious, 2 = not serious |
| seriousness_death | `seriousnessdeath` | C | 1 if the event caused death, otherwise absent |
| seriousness_hospitalization | `seriousnesshospitalization` | C | 1 if it caused or prolonged hospitalization |
| seriousness_life_threatening | `seriousnesslifethreatening` | C | 1 if life-threatening |
| seriousness_disabling | `seriousnessdisabling` | C | 1 if it caused disability |
| seriousness_congenital_anomaly | `seriousnesscongenitalanomali` | C | 1 if it caused a congenital anomaly |
| seriousness_other | `seriousnessother` | C | 1 if other medically important condition |
| occur_country | `occurcountry` | C | ISO country code where the event occurred |
| primary_source_country | `primarysourcecountry` | C | Country of the reporter |
| reporter_qualification | `primarysource.qualification` | C | 1 Physician, 2 Pharmacist, 3 Other health professional, 4 Lawyer, 5 Consumer |
| patient_age | `patient.patientonsetage` | N | Age at onset, in the unit below |
| patient_age_unit | `patient.patientonsetageunit` | C | 800 Decade, 801 Year, 802 Month, 803 Week, 804 Day, 805 Hour |
| patient_sex | `patient.patientsex` | C | 0 Unknown, 1 Male, 2 Female |
| patient_weight_kg | `patient.patientweight` | N | Body weight in kg |
| num_drugs | count of `patient.drug[]` | N | Number of drugs listed in the report |
| num_suspect_drugs | drugs with `drugcharacterization = 1` | N | Number of suspect drugs |
| primary_suspect_drug | `openfda.generic_name[0]` or `medicinalproduct` of the first suspect drug | T | Drug name |
| primary_drug_indication | `drugindication` | T | Why the drug was taken |
| primary_drug_route | `openfda.route[0]` | C | Route of administration |
| num_reactions | count of `patient.reaction[]` | N | Number of reported reactions |
| reactions | `patient.reaction[]` | list | `{reaction_pt: MedDRA preferred term, reaction_outcome: 1-6}` |

## 2. Table `adverse_events` (SQLite, `data/output.db`)

Includes the columns above, after cleaning, plus these derived columns:

| Column | SQL type | Kind | Derivation |
|---|---|---|---|
| safety_report_id | TEXT PK | ID | primary key, so duplicates are impossible |
| receive_date / receipt_date / transmission_date | TEXT | D | ISO `YYYY-MM-DD` |
| receive_month | TEXT | D | `YYYY-MM` of receive_date |
| serious_label | TEXT | C | Serious / Non-serious |
| is_serious | INTEGER | N | 1 / 0 |
| seriousness_* | INTEGER | N | 1 / 0. A missing flag in openFDA means 0 |
| seriousness_criteria_count | INTEGER | N | NumPy sum of the six flags |
| patient_sex | TEXT | C | Male / Female / Unknown |
| reporter_type, report_type | TEXT | C | labels decoded from codes |
| age_years | REAL | N | patient_age × unit factor (NumPy `np.select`), range 0–120 |
| age_group | TEXT | C | Infant (<2), Child (2-11), Adolescent (12-17), Adult (18-44), Middle-aged (45-64), Elderly (65+), Unknown |
| is_polypharmacy | INTEGER | N | 1 if num_drugs ≥ 5 (`np.where`) |
| days_to_latest_info | INTEGER | N | receipt_date − receive_date in days |
| primary_suspect_drug | TEXT | T | upper case, extra spaces and trailing punctuation removed, missing = `UNKNOWN` |
| primary_drug_indication | TEXT | T | sentence case, missing = `Unknown` |
| loaded_at_utc | TEXT | D | time the row was first inserted (audit) |

Only in `data/clean_data.csv` (not stored in SQLite): `age_zscore` (NumPy z-score), `log_num_drugs` (`np.log1p`), `age_missing`, `weight_missing`.

## 3. Table `reactions`

| Column | SQL type | Description |
|---|---|---|
| safety_report_id | TEXT, FK | references `adverse_events` |
| reaction_pt | TEXT | MedDRA preferred term, sentence case |
| reaction_outcome | TEXT | Recovered, Recovering, Not recovered, Recovered with sequelae, Fatal, Unknown |
| PK | | (safety_report_id, reaction_pt) |

## 4. Table `load_runs`

Audit log of each load: `run_no, executed_at_utc, rows_processed, rows_before, rows_after, rows_inserted, rows_updated, reaction_rows_after`.
