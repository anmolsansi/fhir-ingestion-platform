from sqlalchemy import select
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import FHIR_LAST_UPDATED_GTE, FHIR_PAGE_SIZE
from app.core.logging import log
from app.etl.exporter import run_dir, write_ndjson
from app.etl.loader import (
    upsert_conditions,
    upsert_encounters,
    upsert_medication_requests,
    upsert_observations,
    upsert_patients,
)
from app.etl.transform import (
    condition_to_row,
    encounter_to_row,
    medication_request_to_row,
    observation_to_row,
    patient_to_row,
)
from app.fhir.client import FHIRClient
from app.fhir.validators import (
    validate_condition,
    validate_encounter,
    validate_medication_request,
    validate_observation,
    validate_patient,
)
from app.models.tables import Checkpoint, DeadLetter, IngestionRun, Patient


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _start_run(db: Session, resource_type: str) -> tuple[IngestionRun, str]:
    run = IngestionRun(resource_type=resource_type, status="RUNNING", details={})
    db.add(run)
    db.flush()
    run_path = run_dir(run_id=run.id)
    return run, run_path


def _complete_run(run: IngestionRun, status: str, details: dict):
    run.status = status
    run.details = details
    run.finished_at = _now_iso()


def _effective_since(since_checkpoint: str | None) -> str | None:
    floors = [value for value in (since_checkpoint, FHIR_LAST_UPDATED_GTE) if value]
    if not floors:
        return None
    return max(floors)


def _update_max_timestamp(current: str | None, candidate: str | None) -> str | None:
    if candidate and (current is None or candidate > current):
        return candidate
    return current


def _safe_validate(db: Session, items, validator, resource_type: str, dead_letters: list[dict], run_id: int | None):
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
            record = {
                "resource_type": resource_type,
                "resource_id": item.get("id"),
                "stage": "validate",
                "error": str(exc),
                "raw": item,
                "run_id": run_id,
            }
            dead_letters.append(record)
            db.add(
                DeadLetter(
                    run_id=run_id,
                    resource_type=resource_type,
                    resource_id=item.get("id"),
                    stage="validate",
                    error=str(exc)[:1000],
                    raw=item,
                )
            )
    return ok, bad


def _safe_transform(db: Session, items, transformer, resource_type: str, dead_letters: list[dict], run_id: int | None):
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
            record = {
                "resource_type": resource_type,
                "resource_id": item.get("id"),
                "stage": "transform",
                "error": str(exc),
                "raw": item,
                "run_id": run_id,
            }
            dead_letters.append(record)
            db.add(
                DeadLetter(
                    run_id=run_id,
                    resource_type=resource_type,
                    resource_id=item.get("id"),
                    stage="transform",
                    error=str(exc)[:1000],
                    raw=item,
                )
            )
    return rows, errors


def _safe_upsert(
    db: Session,
    rows: list[dict],
    upsert_fn,
    resource_type: str,
    dead_letters: list[dict],
    run_id: int | None,
):
    if not rows:
        return 0, 0
    try:
        with db.begin_nested():
            return upsert_fn(db, rows), 0
    except SQLAlchemyError as exc:
        log.error("load_failed", resource_type=resource_type, error=str(exc))
        for row in rows:
            record = {
                "resource_type": resource_type,
                "resource_id": row.get("id"),
                "stage": "load",
                "error": str(exc),
                "raw": row,
                "run_id": run_id,
            }
            dead_letters.append(record)
            db.add(
                DeadLetter(
                    run_id=run_id,
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
    since = _effective_since(since_checkpoint)

    params = {"_count": FHIR_PAGE_SIZE, "_sort": "_lastUpdated"}
    if since:
        params["_lastUpdated"] = f"ge{since}"

    run, run_path = _start_run(db, resource_type)
    dead_letters: list[dict] = []

    raw = client.search_all(resource_type, params=params)
    write_ndjson(run_path, f"{resource_type}.raw.ndjson", raw)

    valid, invalid = _safe_validate(db, raw, validate_patient, resource_type, dead_letters, run.id)
    write_ndjson(run_path, f"{resource_type}.valid.ndjson", valid)

    rows, transform_errors = _safe_transform(db, valid, patient_to_row, resource_type, dead_letters, run.id)
    upserted, load_errors = _safe_upsert(db, rows, upsert_patients, resource_type, dead_letters, run.id)

    resource_last_updated = {
        item.get("id"): (item.get("meta") or {}).get("lastUpdated") for item in valid
    }
    max_last_updated = since_checkpoint
    if load_errors == 0:
        for row in rows:
            max_last_updated = _update_max_timestamp(max_last_updated, resource_last_updated.get(row.get("id")))

    should_advance = load_errors == 0
    if should_advance and max_last_updated and (since_checkpoint is None or max_last_updated > since_checkpoint):
        _set_checkpoint(db, resource_type, max_last_updated)

    write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

    result = {
        "resource": resource_type,
        "since": since,
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": invalid,
        "transform_errors": transform_errors,
        "upserted": upserted,
        "load_errors": load_errors,
        "dead_letters": len(dead_letters),
        "new_checkpoint": max_last_updated,
        "run_id": run.id,
        "export_path": run_path,
    }

    _complete_run(run, "SUCCESS" if should_advance else "FAILED", result)
    return result


def ingest_observations(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="Observation",
        validator=validate_observation,
        transformer=observation_to_row,
        upserter=upsert_observations,
        patient_id=patient_id,
        extra_params={"code:missing": "false"},
    )


def ingest_encounters(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="Encounter",
        validator=validate_encounter,
        transformer=encounter_to_row,
        upserter=upsert_encounters,
        patient_id=patient_id,
    )


def ingest_conditions(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="Condition",
        validator=validate_condition,
        transformer=condition_to_row,
        upserter=upsert_conditions,
        patient_id=patient_id,
    )


def ingest_medication_requests(db: Session, client: FHIRClient, patient_id: str | None = None) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="MedicationRequest",
        validator=validate_medication_request,
        transformer=medication_request_to_row,
        upserter=upsert_medication_requests,
        patient_id=patient_id,
    )


def _generic_ingest(
    db: Session,
    client: FHIRClient,
    resource_type: str,
    validator,
    transformer,
    upserter,
    patient_id: str | None = None,
    extra_params: dict | None = None,
):
    since_checkpoint = _get_checkpoint(db, resource_type)
    since = _effective_since(since_checkpoint)

    params = {"_count": FHIR_PAGE_SIZE, "_sort": "_lastUpdated"}
    if patient_id:
        params["subject"] = f"Patient/{patient_id}"
    if extra_params:
        params.update(extra_params)
    if since:
        params["_lastUpdated"] = f"ge{since}"

    run, run_path = _start_run(db, resource_type)
    dead_letters: list[dict] = []

    raw = client.search_all(resource_type, params=params)
    write_ndjson(run_path, f"{resource_type}.raw.ndjson", raw)

    valid, invalid = _safe_validate(db, raw, validator, resource_type, dead_letters, run.id)
    write_ndjson(run_path, f"{resource_type}.valid.ndjson", valid)

    rows, transform_errors = _safe_transform(db, valid, transformer, resource_type, dead_letters, run.id)

    patient_ids = {row.get("patient_id") for row in rows if row.get("patient_id")}
    patient_backfill = _ensure_patients_exist(db, client, patient_ids)

    existing_after = {
        pid
        for (pid,) in db.query(Patient.id)
        .filter(Patient.id.in_(list(patient_ids)))
        .all()
    }
    final_rows = [row for row in rows if row.get("patient_id") in existing_after]

    upserted, load_errors = _safe_upsert(db, final_rows, upserter, resource_type, dead_letters, run.id)

    resource_last_updated = {
        item.get("id"): (item.get("meta") or {}).get("lastUpdated") for item in valid
    }
    max_last_updated = since_checkpoint
    if load_errors == 0:
        for row in final_rows:
            max_last_updated = _update_max_timestamp(
                max_last_updated, resource_last_updated.get(row.get("id"))
            )

    skipped_missing = len(rows) - len(final_rows)
    should_advance = load_errors == 0 and skipped_missing == 0
    if should_advance and max_last_updated and (since_checkpoint is None or max_last_updated > since_checkpoint):
        _set_checkpoint(db, resource_type, max_last_updated)

    write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

    result = {
        "resource": resource_type,
        "since": since,
        "fetched": len(raw),
        "validated": len(valid),
        "invalid": invalid,
        "transform_errors": transform_errors,
        "upserted": upserted,
        "patient_backfill": patient_backfill,
        "skipped_missing_patients": skipped_missing,
        "load_errors": load_errors,
        "dead_letters": len(dead_letters),
        "new_checkpoint": max_last_updated,
        "run_id": run.id,
        "export_path": run_path,
    }

    _complete_run(run, "SUCCESS" if should_advance else "FAILED", result)
    return result
