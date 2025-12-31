from datetime import datetime

import requests
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import FHIR_BASE_URL
from app.db.session import SessionLocal
from app.etl.exporter import run_dir, write_ndjson
from app.etl.pipeline import (
    ingest_conditions,
    ingest_encounters,
    ingest_observations,
    ingest_observations_with_backfill,
    ingest_patients,
    replay_deadletters,
)
from app.fhir.client import FHIRClient
from app.models.tables import (
    Condition,
    DeadLetter,
    Encounter,
    IngestionRun,
    MedicationRequest,
    Observation,
    Patient,
)

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


def _duration_seconds(started_at: str | None, finished_at: str | None) -> float | None:
    if not started_at or not finished_at:
        return None
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        end = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0.0, (end - start).total_seconds())


def _build_run_meta(
    job: str | None,
    trigger: str | None,
    attempt: int | None,
    max_attempts: int | None,
    run_group: str | None,
    airflow_dag_id: str | None,
    airflow_task_id: str | None,
    airflow_run_id: str | None,
) -> dict | None:
    meta: dict[str, object] = {}
    if any([job, trigger, attempt, max_attempts, run_group, airflow_dag_id, airflow_task_id, airflow_run_id]):
        meta["trigger"] = trigger or "api"
    if job:
        meta["job"] = job
    if attempt is not None:
        meta["attempt"] = attempt
    if max_attempts is not None:
        meta["max_attempts"] = max_attempts
    if run_group:
        meta["run_group"] = run_group
    airflow: dict[str, str] = {}
    if airflow_dag_id:
        airflow["dag_id"] = airflow_dag_id
    if airflow_task_id:
        airflow["task_id"] = airflow_task_id
    if airflow_run_id:
        airflow["run_id"] = airflow_run_id
    if airflow:
        meta["airflow"] = airflow
    return meta or None


RESOURCE_MODELS = {
    "Patient": Patient,
    "Observation": Observation,
    "Encounter": Encounter,
    "Condition": Condition,
    "MedicationRequest": MedicationRequest,
}


def _normalize_row(model, row, include_raw: bool) -> dict:
    data = {col.name: getattr(row, col.name) for col in model.__table__.columns}
    if not include_raw:
        data.pop("raw", None)
    return data


def _primary_key_column(model):
    columns = list(model.__table__.primary_key.columns)
    return columns[0] if columns else None


@app.get("/health")
def health():
    return {"status": "ok", "fhir_base_url": FHIR_BASE_URL}


@app.post("/ingest/patients")
def ingest_patients_endpoint(
    job: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    attempt: int | None = Query(default=None, ge=1),
    max_attempts: int | None = Query(default=None, ge=1),
    run_group: str | None = Query(default=None),
    airflow_dag_id: str | None = Query(default=None),
    airflow_task_id: str | None = Query(default=None),
    airflow_run_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    client = FHIRClient(FHIR_BASE_URL)
    try:
        run_meta = _build_run_meta(
            job,
            trigger,
            attempt,
            max_attempts,
            run_group,
            airflow_dag_id,
            airflow_task_id,
            airflow_run_id,
        )
        return ingest_patients(db, client, run_meta=run_meta)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Upstream FHIR request failed: {exc}") from exc


@app.post("/ingest/observations")
def ingest_observations_endpoint(
    patient_id: str | None = Query(default=None),
    job: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    attempt: int | None = Query(default=None, ge=1),
    max_attempts: int | None = Query(default=None, ge=1),
    run_group: str | None = Query(default=None),
    airflow_dag_id: str | None = Query(default=None),
    airflow_task_id: str | None = Query(default=None),
    airflow_run_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    client = FHIRClient(FHIR_BASE_URL)
    try:
        run_meta = _build_run_meta(
            job,
            trigger,
            attempt,
            max_attempts,
            run_group,
            airflow_dag_id,
            airflow_task_id,
            airflow_run_id,
        )
        return ingest_observations(db, client, patient_id=patient_id, run_meta=run_meta)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Upstream FHIR request failed: {exc}") from exc


@app.post("/ingest/observations/backfill")
def ingest_observations_backfill_endpoint(
    patient_id: str | None = Query(default=None),
    job: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    attempt: int | None = Query(default=None, ge=1),
    max_attempts: int | None = Query(default=None, ge=1),
    run_group: str | None = Query(default=None),
    airflow_dag_id: str | None = Query(default=None),
    airflow_task_id: str | None = Query(default=None),
    airflow_run_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    client = FHIRClient(FHIR_BASE_URL)
    try:
        run_meta = _build_run_meta(
            job,
            trigger,
            attempt,
            max_attempts,
            run_group,
            airflow_dag_id,
            airflow_task_id,
            airflow_run_id,
        )
        return ingest_observations_with_backfill(db, client, patient_id=patient_id, run_meta=run_meta)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Upstream FHIR request failed: {exc}") from exc


@app.post("/ingest/encounters")
def ingest_encounters_endpoint(
    patient_id: str | None = Query(default=None),
    job: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    attempt: int | None = Query(default=None, ge=1),
    max_attempts: int | None = Query(default=None, ge=1),
    run_group: str | None = Query(default=None),
    airflow_dag_id: str | None = Query(default=None),
    airflow_task_id: str | None = Query(default=None),
    airflow_run_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    client = FHIRClient(FHIR_BASE_URL)
    try:
        run_meta = _build_run_meta(
            job,
            trigger,
            attempt,
            max_attempts,
            run_group,
            airflow_dag_id,
            airflow_task_id,
            airflow_run_id,
        )
        return ingest_encounters(db, client, patient_id=patient_id, run_meta=run_meta)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Upstream FHIR request failed: {exc}") from exc


@app.post("/ingest/conditions")
def ingest_conditions_endpoint(
    patient_id: str | None = Query(default=None),
    job: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    attempt: int | None = Query(default=None, ge=1),
    max_attempts: int | None = Query(default=None, ge=1),
    run_group: str | None = Query(default=None),
    airflow_dag_id: str | None = Query(default=None),
    airflow_task_id: str | None = Query(default=None),
    airflow_run_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    client = FHIRClient(FHIR_BASE_URL)
    try:
        run_meta = _build_run_meta(
            job,
            trigger,
            attempt,
            max_attempts,
            run_group,
            airflow_dag_id,
            airflow_task_id,
            airflow_run_id,
        )
        return ingest_conditions(db, client, patient_id=patient_id, run_meta=run_meta)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Upstream FHIR request failed: {exc}") from exc


@app.post("/export/{resource_type}")
def export_resource(
    resource_type: str,
    run_id: int | None = Query(default=None),
    include_raw: bool = True,
    include_normalized: bool = True,
    limit: int | None = Query(default=None, ge=1),
    offset: int | None = Query(default=None, ge=0),
    db: Session = Depends(get_db),
):
    model = RESOURCE_MODELS.get(resource_type)
    if not model:
        raise HTTPException(status_code=404, detail="Unknown resource type")
    if not include_raw and not include_normalized:
        raise HTTPException(status_code=400, detail="include_raw or include_normalized must be true")

    query = select(model)
    if run_id is not None and hasattr(model, "run_id"):
        query = query.where(model.run_id == run_id)
    primary_key = _primary_key_column(model)
    if primary_key is not None:
        query = query.order_by(primary_key)
    if limit is not None:
        capped = max(1, min(limit, 100000))
        query = query.limit(capped)
    if offset:
        query = query.offset(offset)
    rows = db.execute(query).scalars().all()

    items: list[dict] = []
    if include_normalized:
        for row in rows:
            items.append(_normalize_row(model, row, include_raw=include_raw))
    else:
        for row in rows:
            items.append(row.raw or {})

    export_path = run_dir(run_id=run_id)
    filename = f"{resource_type}.db.ndjson"
    full_path = write_ndjson(export_path, filename, items)
    return {
        "resource_type": resource_type,
        "run_id": run_id,
        "count": len(items),
        "include_raw": include_raw,
        "include_normalized": include_normalized,
        "offset": offset or 0,
        "limit": limit,
        "path": full_path,
    }


@app.get("/runs")
def list_runs(
    limit: int = 20,
    status: str | None = None,
    resource_type: str | None = None,
    job: str | None = None,
    trigger: str | None = None,
    since: str | None = None,
    until: str | None = None,
    include_details: bool = False,
    db: Session = Depends(get_db),
):
    limit = max(1, min(limit, 200))

    query = select(IngestionRun)
    if status:
        query = query.where(IngestionRun.status == status)
    if resource_type:
        query = query.where(IngestionRun.resource_type == resource_type)
    if since:
        query = query.where(IngestionRun.started_at >= since)
    if until:
        query = query.where(IngestionRun.started_at <= until)

    fetch_limit = limit
    if job or trigger:
        fetch_limit = min(max(limit * 5, limit), 500)

    runs = (
        db.execute(query.order_by(IngestionRun.id.desc()).limit(fetch_limit))
        .scalars()
        .all()
    )

    if job or trigger:
        filtered = []
        for run in runs:
            meta = (run.details or {}).get("run_meta", {})
            if job and meta.get("job") != job:
                continue
            if trigger and meta.get("trigger") != trigger:
                continue
            filtered.append(run)
        runs = filtered[:limit]

    out = []
    for run in runs[:limit]:
        item = {
            "run_id": run.id,
            "resource_type": run.resource_type,
            "status": run.status,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "duration_seconds": _duration_seconds(run.started_at, run.finished_at),
        }
        if include_details:
            item["details"] = run.details or {}
        out.append(item)

    return out


@app.get("/runs/metrics")
def run_metrics(
    status: str | None = None,
    resource_type: str | None = None,
    since: str | None = None,
    until: str | None = None,
    db: Session = Depends(get_db),
):
    filters = []
    if status:
        filters.append(IngestionRun.status == status)
    if resource_type:
        filters.append(IngestionRun.resource_type == resource_type)
    if since:
        filters.append(IngestionRun.started_at >= since)
    if until:
        filters.append(IngestionRun.started_at <= until)

    total = db.execute(select(func.count()).select_from(IngestionRun).where(*filters)).scalar_one()

    by_status = {
        row[0]: int(row[1])
        for row in db.execute(
            select(IngestionRun.status, func.count()).where(*filters).group_by(IngestionRun.status)
        ).all()
    }
    by_resource_type = {
        row[0]: int(row[1])
        for row in db.execute(
            select(IngestionRun.resource_type, func.count())
            .where(*filters)
            .group_by(IngestionRun.resource_type)
        ).all()
    }
    deadletters_by_stage = {
        row[0]: int(row[1])
        for row in db.execute(
            select(DeadLetter.stage, func.count())
            .join(IngestionRun, IngestionRun.id == DeadLetter.run_id)
            .where(*filters)
            .group_by(DeadLetter.stage)
        ).all()
    }

    return {
        "total": int(total),
        "by_status": by_status,
        "by_resource_type": by_resource_type,
        "deadletters_by_stage": deadletters_by_stage,
    }


@app.get("/runs/{run_id}")
def get_run(
    run_id: int,
    include_deadletters: bool = True,
    deadletter_limit: int = 25,
    db: Session = Depends(get_db),
):
    run = db.execute(select(IngestionRun).where(IngestionRun.id == run_id)).scalar_one_or_none()

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    stage_counts = (
        db.execute(
            select(DeadLetter.stage, func.count())
            .where(DeadLetter.run_id == run_id)
            .group_by(DeadLetter.stage)
        )
        .all()
    )

    stage_counts_map = {stage: int(cnt) for stage, cnt in stage_counts}
    total_deadletters = sum(stage_counts_map.values())

    resp = {
        "run_id": run.id,
        "resource_type": run.resource_type,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_seconds": _duration_seconds(run.started_at, run.finished_at),
        "details": run.details or {},
        "deadletters": {
            "total": total_deadletters,
            "by_stage": stage_counts_map,
        },
    }
    if "checkpoint_before" in resp["details"]:
        resp["checkpoint_before"] = resp["details"].get("checkpoint_before")
    if "checkpoint_after" in resp["details"]:
        resp["checkpoint_after"] = resp["details"].get("checkpoint_after")

    if include_deadletters:
        deadletter_limit = max(1, min(deadletter_limit, 200))
        deadletters = (
            db.execute(
                select(DeadLetter)
                .where(DeadLetter.run_id == run_id)
                .order_by(DeadLetter.id.desc())
                .limit(deadletter_limit)
            )
            .scalars()
            .all()
        )

        resp["deadletter_sample"] = [
            {
                "id": d.id,
                "resource_type": d.resource_type,
                "resource_id": d.resource_id,
                "stage": d.stage,
                "error": d.error,
                "created_at": d.created_at,
            }
            for d in deadletters
        ]

    return resp


@app.get("/runs/{run_id}/deadletters")
def get_run_deadletters(
    run_id: int,
    limit: int = 50,
    include_raw: bool = False,
    db: Session = Depends(get_db),
):
    limit = max(1, min(limit, 500))

    deadletters = (
        db.execute(
            select(DeadLetter)
            .where(DeadLetter.run_id == run_id)
            .order_by(DeadLetter.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )

    out = []
    for d in deadletters:
        item = {
            "id": d.id,
            "resource_type": d.resource_type,
            "resource_id": d.resource_id,
            "stage": d.stage,
            "error": d.error,
            "created_at": d.created_at,
        }
        if include_raw:
            item["raw"] = d.raw
        out.append(item)
    return out


@app.post("/runs/{run_id}/replay")
def replay_run_deadletters(
    run_id: int,
    stage: str = Query(default="validate"),
    job: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    attempt: int | None = Query(default=None, ge=1),
    max_attempts: int | None = Query(default=None, ge=1),
    run_group: str | None = Query(default=None),
    airflow_dag_id: str | None = Query(default=None),
    airflow_task_id: str | None = Query(default=None),
    airflow_run_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    run = db.execute(select(IngestionRun).where(IngestionRun.id == run_id)).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if stage not in {"validate", "transform", "load"}:
        raise HTTPException(status_code=400, detail="Replay stage must be validate, transform, or load")

    run_meta = _build_run_meta(
        job,
        trigger or "replay",
        attempt,
        max_attempts,
        run_group,
        airflow_dag_id,
        airflow_task_id,
        airflow_run_id,
    )
    try:
        return replay_deadletters(db, run.id, stage=stage, run_meta=run_meta)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.exception_handler(ValidationError)
def pydantic_validation_exception_handler(request, exc: ValidationError):
    return JSONResponse(status_code=422, content={"detail": exc.errors()})
