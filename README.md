# Streaming and Validation Pipeline for Drug Adverse Event Reports

- **Course:** Data Collection & Preparation, SIS 2
- **Data source:** openFDA Drug Adverse Event API (`https://api.fda.gov/drug/event.json`)
- **Team:** Sailaukhan Yenglik · Kassymbay Nazym · Beisenova Dilnaz · Al-Farabiқyzy Asylai
- **Lecture time:** Wednesday 16:00-18:00
- **Repository:** https://github.com/NazymKassymbay/sis2-openfda-pipeline
- 
## 1. Project overview

The pipeline collects real FDA Adverse Event Reporting System (FAERS) reports and passes them through these stages:

```
openFDA API ──► api_client.py ──► data/raw_data.json + raw_data.csv   (original data preserved)
                                       │
                                       ▼
                 producer.py ──► Kafka topic "drug-adverse-events" (3 partitions, JSON)
                                       │
                                       ▼
                 consumer.py ──► data/consumed_messages.json      (stops on END marker / idle timeout)
                                       │
                                       ▼
                validator.py ──► Pandera schema: valid rows continue,
                                 invalid rows go to data/invalid_records.csv
                                       │
                                       ▼
                  cleaner.py ──► pandas + NumPy cleaning, filtering, GroupBy, pivot/melt
                                       │
                                       ▼
                   loader.py ──► SQLite data/output.db (upsert, loaded twice for idempotency)
                                       │
                                       ▼
                 analysis.py ──► EDA statistics + figures/*.png
```

`main.py` runs all stages in order and prints a verification report that compares record counts across stages.

## 2. Software requirements

| Software | Version |
|---|---|
| Python | 3.10 – 3.14 (tested on 3.14) |
| Docker Desktop (includes Docker Compose) | any recent version |
| Internet access | needed to reach `api.fda.gov` |

## 3. Installation

```bash
git clone https://github.com/NazymKassymbay/sis2-openfda-pipeline.git
cd sis2-openfda-pipeline
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # optional, defaults already work
```

## 4. Kafka broker setup and startup

The repository contains `docker-compose.yml` with a single-node **Apache Kafka 3.9.1** broker. It uses the official `apache/kafka` image in KRaft mode, so ZooKeeper is not needed.

```bash
docker compose up -d                        # start the broker on localhost:9092
docker compose ps                           # STATUS should become "healthy"
docker logs sis2-kafka | grep "Kafka Server started"
```

Stop it with `docker compose down`. To also delete stored messages, use `docker compose down -v`.

## 5. Kafka topic configuration

| Setting | Value |
|---|---|
| Topic name | `drug-adverse-events` (`KAFKA_TOPIC`) |
| Partitions | 3 |
| Replication factor | 1 (single broker) |
| Auto-create | disabled on the broker. `producer.py` creates the topic with `AdminClient` |
| Message key | `safety_report_id` |
| Message value | UTF-8 JSON envelope `{message_type, run_id, sequence, produced_at_utc, payload}` |

To check the topic manually:

```bash
docker exec sis2-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic drug-adverse-events
```

## 6. API configuration

openFDA needs no key. Within one date window the API returns reports in received-date order, so the client samples **5 fixed days of every month** (1, 7, 13, 19, 25) of 2024 and requests 10 reports per day. That gives **600 reports** spread over the whole year. You can change this in `.env`:

| Variable | Meaning | Default |
|---|---|---|
| `OPENFDA_API_KEY` | optional key for a higher daily quota | empty |
| `OPENFDA_DATE_FROM` / `OPENFDA_DATE_TO` | `receivedate` window (YYYYMMDD) | 20240101 / 20241231 |
| `SAMPLE_DAYS` | days of each month that are sampled | 1,7,13,19,25 |
| `TARGET_RECORDS` | number of reports to collect | 600 |

Example request sent by `api_client.py`:
`GET https://api.fda.gov/drug/event.json?search=receivedate:[20240107 TO 20240107]&limit=10&skip=0`

## 7. Execution

### 7.1 Full pipeline (recommended)

```bash
docker compose up -d
python main.py                       # collect -> produce -> consume -> validate -> clean -> load x2 -> EDA
python main.py --skip-collection     # rerun everything on the same raw data, without a new API call
```

### 7.2 Stage by stage (exact commands)

Run these from the project root, in this order:

```bash
python -m src.api_client     # 1. collect from openFDA -> data/raw_data.json, data/raw_data.csv
python -m src.producer       # 2. publish records to Kafka -> data/producer_manifest.json
python -m src.consumer       # 3. consume this run's messages -> data/consumed_messages.json
python -m src.validator      # 4. Pandera validation -> data/validated_records.csv, invalid_records.csv
python -m src.cleaner        # 5. validation + cleaning/transformation -> data/clean_data.csv
python -m src.loader         # 6. load into SQLite twice + SQL verification
python -m src.analysis       # 7. EDA + figures/
```

The producer and consumer can also run in two terminals. Start `python -m src.consumer` after the producer has written `data/producer_manifest.json`.

## 8. Database generation and verification

`loader.py` creates `data/output.db` with three tables:

| Table | Key | Purpose |
|---|---|---|
| `adverse_events` | `PRIMARY KEY (safety_report_id)` | one cleaned report per row |
| `reactions` | `PRIMARY KEY (safety_report_id, reaction_pt)`, FK → `adverse_events` | one reaction per row |
| `load_runs` | `run_no` | audit log of each load execution |

Records are written with `INSERT … ON CONFLICT DO UPDATE` and only update rows whose content changed. Because of this, running the load twice creates no duplicates. The verification queries are in `sql/verification_queries.sql`:

```bash
sqlite3 data/output.db < sql/verification_queries.sql
```

## 9. Analysis results and figures

`python -m src.analysis` (or `main.py`) reads the data back from SQLite and writes:

- `figures/fig1_top_reactions.png`: 15 most reported reactions
- `figures/fig2_serious_rate_by_age_sex.png`: serious-report share by age group and sex
- `figures/fig3_age_distribution_by_seriousness.png`: age distribution, serious vs non-serious
- `figures/fig4_seriousness_criteria.png`: seriousness criteria by sex
- `figures/fig5_monthly_serious_share.png`: monthly share of serious, hospitalization and death reports
- `data/eda_*.csv`, `data/analysis_summary.json`: statistics behind the figures

## 10. Expected outputs

After `python main.py` you should see:

| File | Content |
|---|---|
| `data/raw_data.json`, `data/raw_data.csv` | original API data (≥ 200 records, 600 by default) |
| `data/producer_manifest.json` | run id and number of published messages |
| `data/consumed_messages.json` | records received from Kafka |
| `data/invalid_records.csv`, `data/validation_errors.csv` | quarantined rows and the rule each one broke (includes 4 marked test records) |
| `data/clean_data.csv`, `data/clean_reactions.csv` | cleaned data |
| `data/transform_*.csv` | filtering, GroupBy, pivot_table and melt results |
| `data/output.db` | SQLite database |
| `data/sql_verification.txt` | output of all verification queries |
| `data/pipeline_metrics.json` | counts of every stage |
| `figures/*.png` | 5 figures |

## 11. Verifying successful execution

The run ends with a **PIPELINE VERIFICATION REPORT**. In a successful run:

- `Messages published to Kafka` equals `Messages consumed from Kafka` (≥ 200)
- all 4 deliberately invalid test records are detected (`4/4`)
- `Records after cleaning` equals `Records stored in SQLite`
- every consistency check prints `[PASS]`, including `idempotent_reload` (second load: `inserted=0 updated=0`, same table fingerprint)
- the exit code is `0` (`echo $?`)

## 12. Troubleshooting

| Message | Fix |
|---|---|
| `Kafka broker at localhost:9092 is not reachable` | `docker compose up -d`, wait until `docker compose ps` shows *healthy* |
| `HTTP 429` warnings | the client retries with backoff automatically. Add `OPENFDA_API_KEY` if it continues |
| `data/raw_data.json missing` | run `python main.py` once without `--skip-collection` |

## Project structure

```
├── main.py                  pipeline orchestrator
├── docker-compose.yml       Apache Kafka broker
├── requirements.txt
├── .env.example             safe example configuration
├── src/
│   ├── config.py            settings (env variables, paths)
│   ├── metrics.py           stage counters -> data/pipeline_metrics.json
│   ├── api_client.py        openFDA collection + raw data preservation
│   ├── producer.py          Kafka producer (topic creation, JSON serialization)
│   ├── consumer.py          Kafka consumer (JSON deserialization, termination)
│   ├── validator.py         Pandera schema + invalid test records
│   ├── cleaner.py           pandas/NumPy cleaning and transformation
│   ├── loader.py            SQLite schema, idempotent upsert, SQL verification
│   └── analysis.py          EDA and figures
├── sql/verification_queries.sql
├── data/                    pipeline outputs (raw_data.csv, output.db, ...)
├── figures/                 generated plots
├── docs/data_dictionary.md
└── report/project_report.pdf
```
