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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _initial_status(run_meta: dict | None) -> str:
    if run_meta and isinstance(run_meta.get("attempt"), int) and run_meta["attempt"] > 1:
        return "RETRYING"
    return "RUNNING"


def _dead_letter_counts(dead_letters: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in dead_letters:
        stage = record.get("stage", "unknown") or "unknown"
        counts[stage] = counts.get(stage, 0) + 1
    return counts


def _with_run_meta(meta: dict, run_meta: dict | None) -> dict:
    if not run_meta:
        return meta
    merged = dict(meta)
    merged["run_meta"] = run_meta
    return merged


def _start_run(
    db: Session,
    resource_type: str,
    meta: dict | None,
    status: str | None = None,
) -> IngestionRun:
    run = IngestionRun(
        resource_type=resource_type,
        started_at=_utc_now_iso(),
        status=status or "RUNNING",
        details=meta or {},
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def _finish_run(db: Session, run_id: int, status: str, details_patch: dict | None):
    run = db.execute(select(IngestionRun).where(IngestionRun.id == run_id)).scalar_one()
    run.status = status
    run.finished_at = _utc_now_iso()
    existing = run.details or {}
    if details_patch:
        existing.update(details_patch)
    run.details = existing
    db.add(run)
    db.commit()


def _effective_since(since_checkpoint: str | None) -> str | None:
    floors = [value for value in (since_checkpoint, FHIR_LAST_UPDATED_GTE) if value]
    if not floors:
        return None
    return max(floors)


def _update_max_timestamp(current: str | None, candidate) -> str | None:
    current_norm = _normalize_last_updated(current)
    candidate_norm = _normalize_last_updated(candidate)
    if candidate_norm and (current_norm is None or candidate_norm > current_norm):
        return candidate_norm
    return current_norm


def _normalize_last_updated(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return str(value)


def _safe_validate(db: Session, items, validator, resource_type: str, run_id: int, dead_letters: list[dict]):
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


def _safe_transform(db: Session, items, transformer, resource_type: str, run_id: int, dead_letters: list[dict]):
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
    run_id: int,
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


def _ensure_patients_exist(
    db: Session,
    client: FHIRClient,
    patient_ids: set[str],
    run_id: int | None = None,
) -> dict:
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
    if run_id is not None:
        _attach_run_id(rows, run_id)
    upserted = upsert_patients(db, rows)
    return {"requested": len(patient_ids), "fetched": len(fetched), "upserted": upserted}


def _get_checkpoint(db: Session, resource_type: str) -> str | None:
    row = db.execute(select(Checkpoint).where(Checkpoint.resource_type == resource_type)).scalar_one_or_none()
    return row.last_successful_lastupdated if row else None


def _set_checkpoint(db: Session, resource_type: str, last_updated: str):
    normalized = _normalize_last_updated(last_updated)
    row = db.execute(select(Checkpoint).where(Checkpoint.resource_type == resource_type)).scalar_one_or_none()
    if not row:
        row = Checkpoint(resource_type=resource_type, last_successful_lastupdated=normalized)
        db.add(row)
    else:
        row.last_successful_lastupdated = normalized
        row.updated_at = _utc_now_iso()


def _filter_rows_by_patient(
    db: Session,
    resource_type: str,
    run_id: int,
    rows: list[dict],
    dead_letters: list[dict],
    stage: str = "transform",
) -> tuple[list[dict], int]:
    if not rows:
        return [], 0
    patient_ids = {r.get("patient_id") for r in rows if r.get("patient_id")}
    if patient_ids:
        existing = set(
            db.execute(select(Patient.id).where(Patient.id.in_(list(patient_ids))))
            .scalars()
            .all()
        )
    else:
        existing = set()

    final_rows = []
    skipped = 0
    for row in rows:
        pid = row.get("patient_id")
        if pid and pid in existing:
            final_rows.append(row)
            continue
        skipped += 1
        dead_letters.append(
            {
                "run_id": run_id,
                "resource_type": resource_type,
                "resource_id": row.get("id"),
                "stage": stage,
                "error": f"Missing patient_id in DB: {pid}",
                "raw": row,
            }
        )
        db.add(
            DeadLetter(
                run_id=run_id,
                resource_type=resource_type,
                resource_id=row.get("id"),
                stage=stage,
                error=f"Missing patient_id in DB: {pid}"[:1000],
                raw=row,
            )
        )
    return final_rows, skipped


def _attach_run_id(rows: list[dict], run_id: int) -> list[dict]:
    for row in rows:
        row["run_id"] = run_id
    return rows


def ingest_patients(db: Session, client: FHIRClient, run_meta: dict | None = None) -> dict:
    resource_type = "Patient"
    since_checkpoint = _get_checkpoint(db, resource_type)
    since = _effective_since(since_checkpoint)

    params = {"_count": FHIR_PAGE_SIZE, "_sort": "_lastUpdated"}
    if since:
        params["_lastUpdated"] = f"ge{since}"

    run = _start_run(
        db,
        resource_type,
        _with_run_meta(
            {
                "since_checkpoint": since_checkpoint,
                "since_effective": since,
                "params": params,
            },
            run_meta,
        ),
        status=_initial_status(run_meta),
    )
    run_path = run_dir(run_id=run.id)
    dead_letters: list[dict] = []

    try:
        raw = client.search_all(resource_type, params=params)
        write_ndjson(run_path, f"{resource_type}.raw.ndjson", raw)

        valid, invalid = _safe_validate(db, raw, validate_patient, resource_type, run.id, dead_letters)
        write_ndjson(run_path, f"{resource_type}.valid.ndjson", valid)

        rows, transform_errors = _safe_transform(db, valid, patient_to_row, resource_type, run.id, dead_letters)
        _attach_run_id(rows, run.id)
        upserted, load_errors = _safe_upsert(db, rows, upsert_patients, resource_type, dead_letters, run.id)

        resource_last_updated = {
            item.get("id"): (item.get("meta") or {}).get("lastUpdated") for item in valid
        }
        max_last_updated = since_checkpoint
        if load_errors == 0:
            for row in rows:
                max_last_updated = _update_max_timestamp(
                    max_last_updated, resource_last_updated.get(row.get("id"))
                )

        checkpoint_after = since_checkpoint
        should_advance = load_errors == 0 and max_last_updated and (
            since_checkpoint is None or max_last_updated > since_checkpoint
        )
        if should_advance:
            _set_checkpoint(db, resource_type, max_last_updated)
            checkpoint_after = max_last_updated

        write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

        dead_letters_by_stage = _dead_letter_counts(dead_letters)
        result = {
            "run_id": run.id,
            "resource": resource_type,
            "since": since,
            "fetched": len(raw),
            "validated": len(valid),
            "invalid": invalid,
            "transform_errors": transform_errors,
            "upserted": upserted,
            "load_errors": load_errors,
            "checkpoint_before": since_checkpoint,
            "checkpoint_after": checkpoint_after,
            "dead_letters": len(dead_letters),
            "dead_letters_by_stage": dead_letters_by_stage,
            "export_path": run_path,
        }

        _finish_run(db, run.id, "SUCCESS" if load_errors == 0 else "FAILED", result)
        return result
    except Exception as exc:
        db.rollback()
        _finish_run(
            db,
            run.id,
            "FAILED",
            {
                "error": str(exc),
                "checkpoint_before": since_checkpoint,
                "export_path": run_path,
            },
        )
        raise


def ingest_observations(
    db: Session,
    client: FHIRClient,
    patient_id: str | None = None,
    run_meta: dict | None = None,
) -> dict:
    resource_type = "Observation"
    since_checkpoint = _get_checkpoint(db, resource_type)
    since = FHIR_LAST_UPDATED_GTE or since_checkpoint

    params = {"_count": FHIR_PAGE_SIZE, "_sort": "_lastUpdated"}
    if patient_id:
        params["subject"] = f"Patient/{patient_id}"
    if since:
        params["_lastUpdated"] = f"ge{since}"

    run = _start_run(
        db,
        resource_type,
        _with_run_meta(
            {
                "since_checkpoint": since_checkpoint,
                "since_effective": since,
                "patient_filter": patient_id,
                "params": params,
            },
            run_meta,
        ),
        status=_initial_status(run_meta),
    )

    run_path = run_dir(run_id=run.id)
    dead_letters: list[dict] = []

    try:
        raw = client.search_all(resource_type, params=params)
        write_ndjson(run_path, f"{resource_type}.raw.ndjson", raw)

        valid, invalid = _safe_validate(db, raw, validate_observation, resource_type, run.id, dead_letters)
        write_ndjson(run_path, f"{resource_type}.valid.ndjson", valid)

        rows, transform_errors = _safe_transform(
            db, valid, observation_to_row, resource_type, run.id, dead_letters
        )
        _attach_run_id(rows, run.id)
        _attach_run_id(rows, run.id)

        final_rows = []
        skipped_missing_patients = 0
        if rows:
            patient_ids = {r.get("patient_id") for r in rows if r.get("patient_id")}
            if patient_ids:
                existing = set(
                    db.execute(select(Patient.id).where(Patient.id.in_(list(patient_ids))))
                    .scalars()
                    .all()
                )
            else:
                existing = set()

            for r in rows:
                pid = r.get("patient_id")
                if pid and pid in existing:
                    final_rows.append(r)
                else:
                    skipped_missing_patients += 1
                    dead_letters.append(
                        {
                            "run_id": run.id,
                            "resource_type": resource_type,
                            "resource_id": r.get("id"),
                            "stage": "transform",
                            "error": f"Missing patient_id in DB: {pid}",
                            "raw": r,
                        }
                    )
                    db.add(
                        DeadLetter(
                            run_id=run.id,
                            resource_type=resource_type,
                            resource_id=r.get("id"),
                            stage="transform",
                            error=f"Missing patient_id in DB: {pid}"[:1000],
                            raw=r,
                        )
                    )

        upserted, load_errors = _safe_upsert(
            db, final_rows, upsert_observations, resource_type, dead_letters, run.id
        )

        max_last_updated = since_checkpoint
        for item in valid:
            lu = (item.get("meta") or {}).get("lastUpdated")
            max_last_updated = _update_max_timestamp(max_last_updated, lu)

        checkpoint_after = since_checkpoint
        should_advance = (load_errors == 0) and (skipped_missing_patients == 0) and (not patient_id)
        if should_advance and max_last_updated and (
            since_checkpoint is None or max_last_updated > since_checkpoint
        ):
            _set_checkpoint(db, resource_type, max_last_updated)
            checkpoint_after = max_last_updated

        write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

        dead_letters_by_stage = _dead_letter_counts(dead_letters)
        result = {
            "run_id": run.id,
            "resource": resource_type,
            "patient_id": patient_id,
            "since": since,
            "fetched": len(raw),
            "validated": len(valid),
            "invalid": invalid,
            "transform_errors": transform_errors,
            "skipped_missing_patients": skipped_missing_patients,
            "upserted": upserted,
            "load_errors": load_errors,
            "checkpoint_before": since_checkpoint,
            "checkpoint_after": checkpoint_after,
            "dead_letters_by_stage": dead_letters_by_stage,
            "export_path": run_path,
        }

        _finish_run(db, run.id, "SUCCESS", result)
        return result

    except Exception as exc:
        db.rollback()
        _finish_run(
            db,
            run.id,
            "FAILED",
            {
                "patient_id": patient_id,
                "error": str(exc),
                "checkpoint_before": since_checkpoint,
                "export_path": run_path,
            },
        )
        raise


def ingest_observations_with_backfill(
    db: Session,
    client: FHIRClient,
    patient_id: str | None = None,
    run_meta: dict | None = None,
) -> dict:
    resource_type = "Observation"
    since_checkpoint = _get_checkpoint(db, resource_type)
    since = FHIR_LAST_UPDATED_GTE or since_checkpoint

    params = {"_count": FHIR_PAGE_SIZE, "_sort": "_lastUpdated"}
    if patient_id:
        params["subject"] = f"Patient/{patient_id}"
    if since:
        params["_lastUpdated"] = f"ge{since}"

    run = _start_run(
        db,
        resource_type,
        _with_run_meta(
            {
                "since_checkpoint": since_checkpoint,
                "since_effective": since,
                "patient_filter": patient_id,
                "params": params,
            },
            run_meta,
        ),
        status=_initial_status(run_meta),
    )

    run_path = run_dir(run_id=run.id)
    dead_letters: list[dict] = []

    try:
        raw = client.search_all(resource_type, params=params)
        write_ndjson(run_path, f"{resource_type}.raw.ndjson", raw)

        valid, invalid = _safe_validate(db, raw, validate_observation, resource_type, run.id, dead_letters)
        write_ndjson(run_path, f"{resource_type}.valid.ndjson", valid)

        rows, transform_errors = _safe_transform(
            db, valid, observation_to_row, resource_type, run.id, dead_letters
        )

        patient_ids = {row.get("patient_id") for row in rows if row.get("patient_id")}
        patient_backfill = _ensure_patients_exist(db, client, patient_ids, run_id=run.id)

        existing_after = set(
            db.execute(select(Patient.id).where(Patient.id.in_(list(patient_ids))))
            .scalars()
            .all()
        )
        final_rows = [row for row in rows if row.get("patient_id") in existing_after]
        skipped_missing_patients = len(rows) - len(final_rows)

        upserted, load_errors = _safe_upsert(
            db, final_rows, upsert_observations, resource_type, dead_letters, run.id
        )

        max_last_updated = since_checkpoint
        for item in valid:
            lu = (item.get("meta") or {}).get("lastUpdated")
            max_last_updated = _update_max_timestamp(max_last_updated, lu)

        checkpoint_after = since_checkpoint
        should_advance = (
            load_errors == 0
            and skipped_missing_patients == 0
            and (not patient_id)
            and max_last_updated
            and (since_checkpoint is None or max_last_updated > since_checkpoint)
        )
        if should_advance:
            _set_checkpoint(db, resource_type, max_last_updated)
            checkpoint_after = max_last_updated

        write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

        dead_letters_by_stage = _dead_letter_counts(dead_letters)
        result = {
            "run_id": run.id,
            "resource": resource_type,
            "patient_id": patient_id,
            "since": since,
            "fetched": len(raw),
            "validated": len(valid),
            "invalid": invalid,
            "transform_errors": transform_errors,
            "patient_backfill": patient_backfill,
            "skipped_missing_patients": skipped_missing_patients,
            "upserted": upserted,
            "load_errors": load_errors,
            "checkpoint_before": since_checkpoint,
            "checkpoint_after": checkpoint_after,
            "dead_letters_by_stage": dead_letters_by_stage,
            "export_path": run_path,
        }

        _finish_run(
            db,
            run.id,
            "SUCCESS" if (load_errors == 0 and skipped_missing_patients == 0) else "FAILED",
            result,
        )
        return result

    except Exception as exc:
        db.rollback()
        _finish_run(
            db,
            run.id,
            "FAILED",
            {
                "patient_id": patient_id,
                "error": str(exc),
                "checkpoint_before": since_checkpoint,
                "export_path": run_path,
            },
        )
        raise


REPLAY_CONFIGS = {
    "Patient": {
        "validator": validate_patient,
        "transformer": patient_to_row,
        "upserter": upsert_patients,
        "requires_patient": False,
    },
    "Observation": {
        "validator": validate_observation,
        "transformer": observation_to_row,
        "upserter": upsert_observations,
        "requires_patient": True,
    },
    "Encounter": {
        "validator": validate_encounter,
        "transformer": encounter_to_row,
        "upserter": upsert_encounters,
        "requires_patient": True,
    },
    "Condition": {
        "validator": validate_condition,
        "transformer": condition_to_row,
        "upserter": upsert_conditions,
        "requires_patient": True,
    },
    "MedicationRequest": {
        "validator": validate_medication_request,
        "transformer": medication_request_to_row,
        "upserter": upsert_medication_requests,
        "requires_patient": True,
    },
}


def replay_deadletters(
    db: Session,
    source_run_id: int,
    stage: str,
    run_meta: dict | None = None,
) -> dict:
    run = db.execute(select(IngestionRun).where(IngestionRun.id == source_run_id)).scalar_one()
    resource_type = run.resource_type
    config = REPLAY_CONFIGS.get(resource_type)
    if not config:
        raise ValueError(f"Replay is not supported for resource type {resource_type}")

    since_checkpoint = _get_checkpoint(db, resource_type)
    source_deadletters = (
        db.execute(
            select(DeadLetter)
            .where(DeadLetter.run_id == source_run_id)
            .where(DeadLetter.stage == stage)
            .where(DeadLetter.resource_type == resource_type)
        )
        .scalars()
        .all()
    )
    raw = [d.raw for d in source_deadletters if d.raw]
    missing_raw = len(source_deadletters) - len(raw)

    replay_run = _start_run(
        db,
        resource_type,
        _with_run_meta(
            {
                "replay_of_run_id": source_run_id,
                "replay_stage": stage,
                "replay_source_count": len(source_deadletters),
                "replay_raw_count": len(raw),
                "replay_missing_raw": missing_raw,
                "checkpoint_before": since_checkpoint,
            },
            run_meta,
        ),
        status=_initial_status(run_meta),
    )

    run_path = run_dir(run_id=replay_run.id)
    dead_letters: list[dict] = []

    try:
        validated = []
        invalid = 0
        transform_errors = 0
        rows: list[dict] = []
        if stage in {"validate", "transform"}:
            validated, invalid = _safe_validate(
                db,
                raw,
                config["validator"],
                resource_type,
                replay_run.id,
                dead_letters,
            )
            write_ndjson(run_path, f"{resource_type}.valid.ndjson", validated)

            rows, transform_errors = _safe_transform(
                db,
                validated,
                config["transformer"],
                resource_type,
                replay_run.id,
                dead_letters,
            )
        elif stage == "load":
            rows = [item for item in raw if isinstance(item, dict)]
        else:
            raise ValueError("Invalid replay stage")

        _attach_run_id(rows, replay_run.id)

        skipped_missing_patients = 0
        if config["requires_patient"]:
            rows, skipped_missing_patients = _filter_rows_by_patient(
                db,
                resource_type,
                replay_run.id,
                rows,
                dead_letters,
                stage="load" if stage == "load" else "transform",
            )

        upserted, load_errors = _safe_upsert(
            db, rows, config["upserter"], resource_type, dead_letters, replay_run.id
        )

        write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

        dead_letters_by_stage = _dead_letter_counts(dead_letters)
        result = {
            "run_id": replay_run.id,
            "resource": resource_type,
            "replay_of_run_id": source_run_id,
            "replay_stage": stage,
            "replay_source_count": len(source_deadletters),
            "replay_raw_count": len(raw),
            "replay_missing_raw": missing_raw,
            "validated": len(validated),
            "invalid": invalid,
            "transform_errors": transform_errors,
            "skipped_missing_patients": skipped_missing_patients,
            "upserted": upserted,
            "load_errors": load_errors,
            "dead_letters": len(dead_letters),
            "dead_letters_by_stage": dead_letters_by_stage,
            "checkpoint_before": since_checkpoint,
            "checkpoint_after": since_checkpoint,
            "export_path": run_path,
        }

        _finish_run(
            db,
            replay_run.id,
            "SUCCESS" if (load_errors == 0 and skipped_missing_patients == 0) else "FAILED",
            result,
        )
        return result
    except Exception as exc:
        db.rollback()
        _finish_run(
            db,
            replay_run.id,
            "FAILED",
            {
                "replay_of_run_id": source_run_id,
                "replay_stage": stage,
                "error": str(exc),
                "checkpoint_before": since_checkpoint,
                "export_path": run_path,
            },
        )
        raise


def ingest_encounters(
    db: Session,
    client: FHIRClient,
    patient_id: str | None = None,
    run_meta: dict | None = None,
) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="Encounter",
        validator=validate_encounter,
        transformer=encounter_to_row,
        upserter=upsert_encounters,
        patient_id=patient_id,
        run_meta=run_meta,
    )


def ingest_conditions(
    db: Session,
    client: FHIRClient,
    patient_id: str | None = None,
    run_meta: dict | None = None,
) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="Condition",
        validator=validate_condition,
        transformer=condition_to_row,
        upserter=upsert_conditions,
        patient_id=patient_id,
        run_meta=run_meta,
    )


def ingest_medication_requests(
    db: Session,
    client: FHIRClient,
    patient_id: str | None = None,
    run_meta: dict | None = None,
) -> dict:
    return _generic_ingest(
        db=db,
        client=client,
        resource_type="MedicationRequest",
        validator=validate_medication_request,
        transformer=medication_request_to_row,
        upserter=upsert_medication_requests,
        patient_id=patient_id,
        run_meta=run_meta,
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
    run_meta: dict | None = None,
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

    run = _start_run(
        db,
        resource_type,
        _with_run_meta(
            {
                "since_checkpoint": since_checkpoint,
                "since_effective": since,
                "params": params,
                "patient_id": patient_id,
            },
            run_meta,
        ),
        status=_initial_status(run_meta),
    )
    run_path = run_dir(run_id=run.id)
    dead_letters: list[dict] = []

    try:
        raw = client.search_all(resource_type, params=params)
        write_ndjson(run_path, f"{resource_type}.raw.ndjson", raw)

        valid, invalid = _safe_validate(db, raw, validator, resource_type, run.id, dead_letters)
        write_ndjson(run_path, f"{resource_type}.valid.ndjson", valid)

        rows, transform_errors = _safe_transform(db, valid, transformer, resource_type, run.id, dead_letters)
        _attach_run_id(rows, run.id)

        patient_ids = {row.get("patient_id") for row in rows if row.get("patient_id")}
        patient_backfill = _ensure_patients_exist(db, client, patient_ids, run_id=run.id)

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
        should_advance = (
            load_errors == 0
            and skipped_missing == 0
            and max_last_updated
            and (since_checkpoint is None or max_last_updated > since_checkpoint)
        )
        checkpoint_after = since_checkpoint
        if should_advance:
            _set_checkpoint(db, resource_type, max_last_updated)
            checkpoint_after = max_last_updated

        write_ndjson(run_path, f"{resource_type}.deadletters.ndjson", dead_letters)

        dead_letters_by_stage = _dead_letter_counts(dead_letters)
        result = {
            "run_id": run.id,
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
            "dead_letters_by_stage": dead_letters_by_stage,
            "checkpoint_before": since_checkpoint,
            "checkpoint_after": checkpoint_after,
            "export_path": run_path,
        }

        _finish_run(
            db,
            run.id,
            "SUCCESS" if (load_errors == 0 and skipped_missing == 0) else "FAILED",
            result,
        )
        return result
    except Exception as exc:
        db.rollback()
        _finish_run(
            db,
            run.id,
            "FAILED",
            {
                "error": str(exc),
                "checkpoint_before": since_checkpoint,
                "export_path": run_path,
            },
        )
        raise
