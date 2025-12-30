from sqlalchemy import select
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import FHIR_LAST_UPDATED_GTE, FHIR_PAGE_SIZE
from app.core.logging import log
from app.etl.loader import upsert_observations, upsert_patients
from app.etl.transform import observation_to_row, patient_to_row
from app.fhir.client import FHIRClient
from app.fhir.validators import validate_observation, validate_patient
from app.models.tables import Checkpoint, DeadLetter, Patient


def _safe_validate(db: Session, items, validator, resource_type: str):
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
            db.add(
                DeadLetter(
                    run_id=None,
                    resource_type=resource_type,
                    resource_id=item.get("id"),
                    stage="validate",
                    error=str(exc)[:1000],
                    raw=item,
                )
            )
    return ok, bad


def _safe_transform(db: Session, items, transformer, resource_type: str):
    rows = []
    errors = 0
    for item in items:
        try:
            rows.append(transformer(item))
        except Exception as exc:
            errors += 1
            log.warning(
                "transform_failed",
                resource_type=resource_type,
                id=item.get("id"),
                error=str(exc),
            )
            db.add(
                DeadLetter(
                    run_id=None,
                    resource_type=resource_type,
                    resource_id=item.get("id"),
                    stage="transform",
                    error=str(exc)[:1000],
                    raw=item,
                )
            )
    return rows, errors


def _safe_upsert(db: Session, rows: list[dict], upsert_fn, resource_type: str):
    if not rows:
        return 0, 0
    try:
        return upsert_fn(db, rows), 0
    except Exception as exc:
        log.error("load_failed", resource_type=resource_type, error=str(exc))
        for row in rows:
            db.add(
                DeadLetter(
                    run_id=None,
                    resource_type=resource_type,
                    resource_id=row.get("id"),
                    stage="load",
                    error=str(exc)[:1000],
                    raw=row,
                )
            )
        return 0, len(rows)


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


def _get_checkpoint(db: Session, resource_type: str) -> str | None:
    row = db.execute(select(Checkpoint).where(Checkpoint.resource_type == resource_type)).scalar_one_or_none()
    return row.last_successful_lastupdated if row else None


def _set_checkpoint(db: Session, resource_type: str, last_updated: str):
    row = db.execute(select(Checkpoint).where(Checkpoint.resource_type == resource_type)).scalar_one_or_none()
    if not row:
        row = Checkpoint(resource_type=resource_type, last_successful_lastupdated=last_updated)
        db.add(row)
    else:
        row.last_successful_lastupdated = last_updated


def ingest_patients(db: Session, client: FHIRClient) -> dict:
    resource_type = "Patient"
    since_checkpoint = _get_checkpoint(db, resource_type)
    since = FHIR_LAST_UPDATED_GTE or since_checkpoint

    params = {"_count": FHIR_PAGE_SIZE, "_sort": "_lastUpdated"}
    if since:
        params["_lastUpdated"] = f"ge{since}"

    raw = client.search_all(resource_type, params=params)
    valid, invalid = _safe_validate(db, raw, validate_patient, resource_type)
    rows, transform_errors = _safe_transform(db, valid, patient_to_row, resource_type)
    upserted, load_errors = _safe_upsert(db, rows, upsert_patients, resource_type)

    max_last_updated = since_checkpoint
    for item in valid:
        last_updated = (item.get("meta") or {}).get("lastUpdated")
        if last_updated and (max_last_updated is None or last_updated > max_last_updated):
            max_last_updated = last_updated

    if max_last_updated and (since_checkpoint is None or max_last_updated > since_checkpoint):
        _set_checkpoint(db, resource_type, max_last_updated)

    return {
        "resource": resource_type,
        "since": since,
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": invalid,
        "transform_errors": transform_errors,
        "upserted": upserted,
        "load_errors": load_errors,
        "new_checkpoint": max_last_updated,
    }


def ingest_observations(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    resource_type = "Observation"
    since_checkpoint = _get_checkpoint(db, resource_type)
    since = FHIR_LAST_UPDATED_GTE or since_checkpoint

    params = {"_count": FHIR_PAGE_SIZE}

    if patient_id:
        params["subject"] = f"Patient/{patient_id}"

    params["code:missing"] = "false"

    params["_sort"] = "_lastUpdated"

    if since:
        params["_lastUpdated"] = f"ge{since}"

    raw = client.search_all(resource_type, params=params)
    valid, invalid = _safe_validate(db, raw, validate_observation, resource_type)
    rows, transform_errors = _safe_transform(db, valid, observation_to_row, resource_type)

    patient_ids = {row["patient_id"] for row in rows if row.get("patient_id")}
    patient_backfill = _ensure_patients_exist(db, client, patient_ids)

    existing_after = {
        pid
        for (pid,) in db.query(Patient.id)
        .filter(Patient.id.in_(list(patient_ids)))
        .all()
    }
    final_rows = [row for row in rows if row.get("patient_id") in existing_after]

    upserted, load_errors = _safe_upsert(db, final_rows, upsert_observations, resource_type)

    max_last_updated = since_checkpoint
    for item in valid:
        last_updated = (item.get("meta") or {}).get("lastUpdated")
        if last_updated and (max_last_updated is None or last_updated > max_last_updated):
            max_last_updated = last_updated

    if max_last_updated and (since_checkpoint is None or max_last_updated > since_checkpoint):
        _set_checkpoint(db, resource_type, max_last_updated)

    return {
        "resource": resource_type,
        "since": since,
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": invalid,
        "transform_errors": transform_errors,
        "upserted": upserted,
        "patient_backfill": patient_backfill,
        "skipped_missing_patients": len(rows) - len(final_rows),
        "load_errors": load_errors,
        "new_checkpoint": max_last_updated,
    }
