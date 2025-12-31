#!/usr/bin/env bash
set -euo pipefail

docker compose up --build -d
docker compose exec api python scripts/init_db.py

curl -s http://localhost:8000/health | jq .
curl -s -X POST http://localhost:8000/ingest/patients | jq .
curl -s -X POST http://localhost:8000/ingest/observations | jq .

echo "DB counts:"
docker compose exec db psql -U postgres -d fhir -c "select count(*) as patients from patients;"
docker compose exec db psql -U postgres -d fhir -c "select count(*) as observations from observations;"
