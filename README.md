# fhir-ingestion-platform

Think of this project as a **data conveyor belt** for healthcare data. It pulls FHIR data from a remote server, checks it, cleans it, and stores it in your own database so you can build dashboards, analytics, or ML models without constantly hitting a live hospital system.

It also keeps a **run log** for everything it does, so you can answer:
- What ran?
- When did it run?
- What failed?
- Can I replay just the failures?

## What problem does it solve (simple)
FHIR servers are great for live data, but not great for analytics:
- They can be slow or go down.
- Data can be messy or incomplete.
- If one record is bad, a whole batch can fail.

This project fixes that by making ingestion **reliable and observable**:
- **Pulls** data from FHIR.
- **Validates** it so bad data doesn’t poison your DB.
- **Transforms** it into clean tables.
- **Stores raw JSON too** so you can debug.
- **Logs every run** and all errors.
- **Lets you replay** failed records later.

## What issues this app is designed to solve (in real life)
If you are a developer, analyst, or data team working with healthcare data, you often hit these problems:
- "The FHIR server is slow or down, so our dashboard fails."
- "We need clean tables for analytics, not messy JSON blobs."
- "One bad record broke our ingestion job."
- "We don’t know what happened in last night’s job."
- "We want to retry only the failures, not everything."

This app is built to solve those exact issues by:
- **Caching data in your own DB**, so you are not blocked by upstream outages.
- **Keeping clean tables** that are easy to query.
- **Logging every run**, so you can audit and debug quickly.
- **Separating failures into dead letters**, so one bad record does not kill the run.
- **Supporting replay**, so you can fix and retry only broken items.

## What the app actually contains
### 1) API server
You call HTTP endpoints like `/ingest/patients`.
These endpoints start a run and return a summary.

### 2) Pipeline
The pipeline is the "factory line."
It does: fetch → validate → transform → load → export.

### 3) Database tables
Stores both:
- **Normalized rows** (query-friendly)
- **Raw JSON** (debug-friendly)
Also stores:
- **Runs** (audit history)
- **Dead letters** (failed items)
- **Checkpoints** (last successful `_lastUpdated`)

### 4) Scheduler
A cron container calls ingestion endpoints on a schedule.
Optional Airflow DAG exists if you want more control.

## Quick start (Docker)
1) Build and start containers:
```bash
docker compose up -d --build
```

2) Create tables:
```bash
docker compose exec api python /app/scripts/init_db.py
```

3) Apply migrations (indexes + run_id columns):
```bash
docker compose exec api python /app/scripts/migrate_db.py
```

4) Check that the API is alive:
```bash
curl http://localhost:8000/health
```

## Environment variables (what they mean)
Set these in your shell or `.env` (see `.env.example`):
- `FHIR_BASE_URL`: where to pull FHIR data from
- `DB_URL`: Postgres connection string
- `FHIR_PAGE_SIZE`: how many items per page from FHIR
- `FHIR_LAST_UPDATED_GTE`: optional minimum `_lastUpdated` (start point)
- `ALERT_WEBHOOK_URL`: optional webhook for cron failure alerts

## How ingestion works (step by step)
When you hit an ingest endpoint:
1) **Create a run record** in `ingestion_runs` (status = RUNNING)
2) **Fetch raw FHIR data** from the remote server
3) **Validate each item**
   - bad items → dead letters
4) **Transform valid items** into rows for Postgres
5) **Load (upsert)** into the DB
6) **Export files** (raw, valid, dead letters)
7) **Update checkpoint** if the run succeeded
8) **Mark run** SUCCESS or FAILED

## Endpoints (your "buttons")
### Ingest
- `POST /ingest/patients`
- `POST /ingest/observations`
- `POST /ingest/observations/backfill`
  Fetches missing patients first, then loads observations.
- `POST /ingest/encounters`
- `POST /ingest/conditions`

### Run observability
- `GET /runs?resource_type=Observation&limit=20`
  Get recent runs.
- `GET /runs/{run_id}`
  Details about a run + dead-letter counts.
- `GET /runs/{run_id}/deadletters?limit=200`
  List the failed items.
- `POST /runs/{run_id}/replay?stage=validate|transform|load`
  Retry failed items.

### Export from DB
- `POST /export/{resource_type}?run_id=123&include_raw=true&include_normalized=true`
  Dumps rows from Postgres into NDJSON files.

## Dead letters (why they matter)
If a record fails:
- It does **not** crash the run.
- It goes into the `dead_letters` table.
- You can replay it later after fixing the cause.

## Replay (fix bad records)
Example:
```
POST /runs/42/replay?stage=validate
```
This creates a **new run**, tied back to the old one with `replay_of_run_id`.

## Checkpoints (incremental ingestion)
Checkpoints store the last successful `_lastUpdated`.
This means:
- Next run starts where the last one ended.
- You don’t reload old data.

## Scheduling
Cron is built in and calls ingestion endpoints on a schedule.
- File: `scripts/cronjobs`
- Service: `scheduler` in `docker-compose.yml`

Optional Airflow DAG: `airflow/dags/fhir_ingest.py`

## Files written to disk
Each run writes NDJSON files here:
```
exports/run_<run_id>_<timestamp>/
```
Example files:
- `Patient.raw.ndjson`
- `Patient.valid.ndjson`
- `Patient.deadletters.ndjson`

## Tests
Install dependencies:
```bash
pip install -r requirements.txt
```
Run tests:
```bash
pytest -q
```

## Common issues (and fixes)
- **FHIR server returns 500**: retry or change `FHIR_BASE_URL`
- **Migration errors**: run `init_db.py` then `migrate_db.py`
- **Cron alerts not working**: set `ALERT_WEBHOOK_URL`

## Folder map (where things live)
- `app/api/`: FastAPI endpoints
- `app/etl/`: pipeline, transform, loader
- `app/models/`: database tables
- `scripts/`: DB init/migrate, cron config
- `airflow/`: Airflow DAG (optional)
- `exports/`: output files
