from fastapi import Depends, FastAPI, Query
from sqlalchemy.orm import Session

from app.core.config import FHIR_BASE_URL
from app.db.session import SessionLocal
from app.etl.pipeline import ingest_observations, ingest_patients
from app.fhir.client import FHIRClient

app = FastAPI(title="FHIR Ingestion Platform", version="0.1.0")


def get_db():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@app.get("/health")
def health():
    return {"status": "ok", "fhir_base_url": FHIR_BASE_URL}


@app.post("/ingest/patients")
def ingest_patients_endpoint(db: Session = Depends(get_db)):
    client = FHIRClient(FHIR_BASE_URL)
    return ingest_patients(db, client)


@app.post("/ingest/observations")
def ingest_observations_endpoint(
    patient_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    client = FHIRClient(FHIR_BASE_URL)
    return ingest_observations(db, client, patient_id=patient_id)
