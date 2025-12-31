from sqlalchemy import text

from app.db.session import engine


TABLE_STATEMENTS = {
    "patients": [
        "ALTER TABLE patients ADD COLUMN IF NOT EXISTS run_id INTEGER",
        "CREATE INDEX IF NOT EXISTS ix_patients_run_id ON patients (run_id)",
    ],
    "observations": [
        "ALTER TABLE observations ADD COLUMN IF NOT EXISTS run_id INTEGER",
        "CREATE INDEX IF NOT EXISTS ix_observations_run_id ON observations (run_id)",
    ],
    "encounters": [
        "ALTER TABLE encounters ADD COLUMN IF NOT EXISTS run_id INTEGER",
        "CREATE INDEX IF NOT EXISTS ix_encounters_run_id ON encounters (run_id)",
    ],
    "conditions": [
        "ALTER TABLE conditions ADD COLUMN IF NOT EXISTS run_id INTEGER",
        "CREATE INDEX IF NOT EXISTS ix_conditions_run_id ON conditions (run_id)",
    ],
    "medication_requests": [
        "ALTER TABLE medication_requests ADD COLUMN IF NOT EXISTS run_id INTEGER",
        "CREATE INDEX IF NOT EXISTS ix_medication_requests_run_id ON medication_requests (run_id)",
    ],
    "ingestion_runs": [
        "CREATE INDEX IF NOT EXISTS ix_ingestion_runs_started_at ON ingestion_runs (started_at)",
        "CREATE INDEX IF NOT EXISTS ix_ingestion_runs_status ON ingestion_runs (status)",
    ],
    "dead_letters": [
        "CREATE INDEX IF NOT EXISTS ix_dead_letters_run_id ON dead_letters (run_id)",
        "CREATE INDEX IF NOT EXISTS ix_dead_letters_stage ON dead_letters (stage)",
    ],
}


def _table_exists(conn, table_name: str) -> bool:
    result = conn.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :name"
        ),
        {"name": table_name},
    )
    return result.first() is not None


def main():
    with engine.begin() as conn:
        for table_name, statements in TABLE_STATEMENTS.items():
            if not _table_exists(conn, table_name):
                continue
            for stmt in statements:
                conn.execute(text(stmt))
    print("DB migration completed.")


if __name__ == "__main__":
    main()
