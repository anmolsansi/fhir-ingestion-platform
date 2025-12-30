from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import FHIR_LAST_UPDATED_GTE, FHIR_PAGE_SIZE
from app.etl.loader import upsert_observations, upsert_patients
from app.etl.transform import observation_to_row, patient_to_row
from app.fhir.client import FHIRClient
from app.fhir.validators import validate_observation, validate_patient
from app.models.tables import Patient


def _safe_validate(items, validator, resource_type: str):
    ok = []
    bad = []
    for item in items:
        try:
            ok.append(validator(item))
        except Exception as exc:
            bad.append(
                {
                    "resource_type": resource_type,
                    "id": item.get("id"),
                    "error": str(exc),
                }
            )
    return ok, bad


def ingest_patients(db: Session, client: FHIRClient) -> dict:
    params = {"_count": FHIR_PAGE_SIZE}
    if FHIR_LAST_UPDATED_GTE:
        params["_lastUpdated"] = f"ge{FHIR_LAST_UPDATED_GTE}"

    raw = client.search_all("Patient", params=params)
    valid, invalid = _safe_validate(raw, validate_patient, "Patient")
    rows = [patient_to_row(p) for p in valid]
    upserted = upsert_patients(db, rows)

    return {
        "resource": "Patient",
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": len(invalid),
        "upserted": upserted,
        "invalid_samples": invalid[:5],
    }


def ingest_observations(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    params = {"_count": FHIR_PAGE_SIZE}

    if patient_id:
        params["subject"] = f"Patient/{patient_id}"

    params["code:missing"] = "false"

    if FHIR_LAST_UPDATED_GTE:
        params["_lastUpdated"] = f"ge{FHIR_LAST_UPDATED_GTE}"

    raw = client.search_all("Observation", params=params)
    valid, invalid = _safe_validate(raw, validate_observation, "Observation")
    rows = [observation_to_row(o) for o in valid]
    rows, missing_patients = _filter_observations_with_existing_patients(db, rows)
    upserted = upsert_observations(db, rows)

    return {
        "resource": "Observation",
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": len(invalid),
        "upserted": upserted,
        "invalid_samples": invalid[:5],
        "missing_patients": missing_patients[:5],
    }


def _filter_observations_with_existing_patients(db: Session, rows: list[dict]):
    missing = []
    patient_ids = {row["patient_id"] for row in rows if row.get("patient_id")}
    if not patient_ids:
        return rows, missing

    existing = set(db.execute(select(Patient.id).where(Patient.id.in_(patient_ids))).scalars())

    filtered = []
    for row in rows:
        pid = row.get("patient_id")
        if not pid or pid not in existing:
            missing.append({"observation_id": row.get("id"), "patient_id": pid})
            continue
        filtered.append(row)

    return filtered, missing
