from sqlalchemy.orm import Session

from app.core.config import FHIR_LAST_UPDATED_GTE, FHIR_PAGE_SIZE
from app.etl.loader import upsert_observations, upsert_patients
from app.etl.transform import observation_to_row, patient_to_row
from app.fhir.client import FHIRClient
from app.fhir.validators import validate_observation, validate_patient


def ingest_patients(db: Session, client: FHIRClient) -> dict:
    params = {"_count": FHIR_PAGE_SIZE}
    if FHIR_LAST_UPDATED_GTE:
        params["_lastUpdated"] = f"ge{FHIR_LAST_UPDATED_GTE}"

    raw_patients = client.search_all("Patient", params=params)
    valid = [validate_patient(p) for p in raw_patients]
    rows = [patient_to_row(p) for p in valid]
    n = upsert_patients(db, rows)
    return {"fetched": len(raw_patients), "upserted": n}


def ingest_observations(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    params = {"_count": FHIR_PAGE_SIZE}
    if patient_id:
        params["subject"] = f"Patient/{patient_id}"
    if FHIR_LAST_UPDATED_GTE:
        params["_lastUpdated"] = f"ge{FHIR_LAST_UPDATED_GTE}"

    raw_obs = client.search_all("Observation", params=params)
    valid = [validate_observation(o) for o in raw_obs]
    rows = [observation_to_row(o) for o in valid]
    n = upsert_observations(db, rows)
    return {"fetched": len(raw_obs), "upserted": n}
