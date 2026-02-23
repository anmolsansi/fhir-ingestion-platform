from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import Session, sessionmaker

from app.api.main import app, get_db
from app.models.base import Base
from app.models.tables import DeadLetter, IngestionRun, Observation


def _make_session() -> Session:
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)()


def test_export_filters_by_run_id():
    db = _make_session()
    run = IngestionRun(resource_type="Observation", status="SUCCESS", details={})
    db.add(run)
    db.commit()
    db.refresh(run)
    run_id = run.id

    db.add_all(
        [
            Observation(
                id="obs-1",
                patient_id="p1",
                run_id=run.id,
                status="final",
                raw={"id": "obs-1"},
            ),
            Observation(
                id="obs-2",
                patient_id="p1",
                run_id=None,
                status="final",
                raw={"id": "obs-2"},
            ),
        ]
    )
    db.commit()

    def override_get_db():
        try:
            yield db
            db.commit()
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    resp = client.post(f"/export/Observation?run_id={run_id}&include_raw=false&include_normalized=true")
    app.dependency_overrides.clear()

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["count"] == 1
    assert payload["run_id"] == run_id


def test_replay_validate_with_empty_raw():
    db = _make_session()
    run = IngestionRun(resource_type="Observation", status="FAILED", details={})
    db.add(run)
    db.commit()
    db.refresh(run)
    run_id = run.id

    db.add(
        DeadLetter(
            run_id=run_id,
            resource_type="Observation",
            resource_id="obs-1",
            stage="validate",
            error="bad payload",
            raw=None,
        )
    )
    db.commit()

    def override_get_db():
        try:
            yield db
            db.commit()
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    resp = client.post(f"/runs/{run_id}/replay?stage=validate")
    app.dependency_overrides.clear()

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["replay_of_run_id"] == run_id
    assert payload["replay_stage"] == "validate"


def test_deadletters_support_stage_filter_and_offset():
    db = _make_session()
    run = IngestionRun(resource_type="Observation", status="FAILED", details={})
    db.add(run)
    db.commit()
    db.refresh(run)

    db.add_all(
        [
            DeadLetter(
                run_id=run.id,
                resource_type="Observation",
                resource_id="obs-1",
                stage="validate",
                error="validate failed",
                raw={"id": "obs-1"},
            ),
            DeadLetter(
                run_id=run.id,
                resource_type="Observation",
                resource_id="obs-2",
                stage="transform",
                error="transform failed",
                raw={"id": "obs-2"},
            ),
            DeadLetter(
                run_id=run.id,
                resource_type="Observation",
                resource_id="obs-3",
                stage="validate",
                error="validate failed again",
                raw={"id": "obs-3"},
            ),
        ]
    )
    db.commit()

    def override_get_db():
        try:
            yield db
            db.commit()
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)

    filtered = client.get(f"/runs/{run.id}/deadletters?stage=validate")
    paged = client.get(f"/runs/{run.id}/deadletters?limit=1&offset=1")

    app.dependency_overrides.clear()

    assert filtered.status_code == 200
    filtered_payload = filtered.json()
    assert len(filtered_payload) == 2
    assert all(item["stage"] == "validate" for item in filtered_payload)

    assert paged.status_code == 200
    paged_payload = paged.json()
    assert len(paged_payload) == 1
