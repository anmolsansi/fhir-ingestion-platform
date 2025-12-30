from sqlalchemy.orm import Session

from app.core.config import FHIR_LAST_UPDATED_GTE, FHIR_PAGE_SIZE
from app.core.logging import log
from app.etl.loader import upsert_observations, upsert_patients
from app.etl.transform import observation_to_row, patient_to_row
from app.fhir.client import FHIRClient
from app.fhir.validators import validate_observation, validate_patient
from app.models.tables import Patient


def _safe_validate(items, validator, resource_type: str):
    ok = []
    bad = 0
    for item in items:
        try:
            ok.append(validator(item))
        except Exception as exc:
            bad += 1
            log.warning(
                "validation_failed",
                resource_type=resource_type,
                id=item.get("id"),
                error=str(exc),
            )
    return ok, bad


def _ensure_patients_exist(db: Session, client: FHIRClient, patient_ids: set[str]) -> dict:
    if not patient_ids:
        return {"requested": 0, "fetched": 0, "upserted": 0}

    existing = {
        pid
        for (pid,) in db.query(Patient.id)
        .filter(Patient.id.in_(list(patient_ids)))
        .all()
    }
    missing = sorted(list(patient_ids - existing))
    fetched = []
    for pid in missing:
        try:
            raw = client.get(f"/Patient/{pid}")
            fetched.append(validate_patient(raw))
        except Exception:
            continue

    rows = [patient_to_row(p) for p in fetched]
    upserted = upsert_patients(db, rows)
    return {"requested": len(patient_ids), "fetched": len(fetched), "upserted": upserted}


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
        "invalid": invalid,
        "upserted": upserted,
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

    patient_ids = {row["patient_id"] for row in rows if row.get("patient_id")}
    patient_backfill = _ensure_patients_exist(db, client, patient_ids)

    existing_after = {
        pid
        for (pid,) in db.query(Patient.id)
        .filter(Patient.id.in_(list(patient_ids)))
        .all()
    }
    final_rows = [row for row in rows if row.get("patient_id") in existing_after]

    upserted = upsert_observations(db, final_rows)

    return {
        "resource": "Observation",
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": invalid,
        "upserted": upserted,
        "patient_backfill": patient_backfill,
        "skipped_missing_patients": len(rows) - len(final_rows),
    }
