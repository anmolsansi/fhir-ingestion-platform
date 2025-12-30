import os

from dotenv import load_dotenv

load_dotenv()

FHIR_BASE_URL = os.getenv("FHIR_BASE_URL", "https://hapi.fhir.org/baseR4")
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@db:5432/fhir")

REQUEST_TIMEOUT_SECS = float(os.getenv("REQUEST_TIMEOUT_SECS", "30"))
FHIR_PAGE_SIZE = int(os.getenv("FHIR_PAGE_SIZE", "100"))

# Optional checkpoint. If set, we do incremental ingestion using _lastUpdated.
FHIR_LAST_UPDATED_GTE = os.getenv("FHIR_LAST_UPDATED_GTE")  # e.g. "2025-01-01T00:00:00Z"
