import json

from fastapi.encoders import jsonable_encoder
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.tables import Condition, Encounter, MedicationRequest, Observation, Patient


def upsert_patients(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0

    for row in rows:
        if "raw" in row:
            row["raw"] = jsonable_encoder(row["raw"])

    for i, row in enumerate(rows[:3]):
        try:
            json.dumps(row["raw"])
        except TypeError as exc:
            print("BAD ROW INDEX:", i)
            print("ERROR:", exc)
            print("RAW TYPE:", type(row["raw"]))
            print("RAW KEYS:", list(row["raw"].keys()) if isinstance(row["raw"], dict) else None)
            raise

    stmt = insert(Patient).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Patient.id],
        set_={
            "family": stmt.excluded.family,
            "given": stmt.excluded.given,
            "gender": stmt.excluded.gender,
            "birth_date": stmt.excluded.birth_date,
            "raw": stmt.excluded.raw,
        },
    )
    res = db.execute(stmt)
    return res.rowcount or 0


def upsert_observations(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0

    for row in rows:
        if "raw" in row:
            row["raw"] = jsonable_encoder(row["raw"])

    stmt = insert(Observation).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Observation.id],
        set_={
            "patient_id": stmt.excluded.patient_id,
            "status": stmt.excluded.status,
            "code": stmt.excluded.code,
            "code_display": stmt.excluded.code_display,
            "effective_datetime": stmt.excluded.effective_datetime,
            "value_quantity": stmt.excluded.value_quantity,
            "value_unit": stmt.excluded.value_unit,
            "value_string": stmt.excluded.value_string,
            "raw": stmt.excluded.raw,
        },
    )
    res = db.execute(stmt)
    return res.rowcount or 0


def upsert_encounters(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = insert(Encounter).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Encounter.id],
        set_={
            "patient_id": stmt.excluded.patient_id,
            "status": stmt.excluded.status,
            "encounter_class": stmt.excluded.encounter_class,
            "start": stmt.excluded.start,
            "end": stmt.excluded.end,
            "raw": stmt.excluded.raw,
        },
    )
    res = db.execute(stmt)
    return res.rowcount or 0


def upsert_conditions(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = insert(Condition).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Condition.id],
        set_={
            "patient_id": stmt.excluded.patient_id,
            "clinical_status": stmt.excluded.clinical_status,
            "verification_status": stmt.excluded.verification_status,
            "code": stmt.excluded.code,
            "onset": stmt.excluded.onset,
            "recorded_date": stmt.excluded.recorded_date,
            "raw": stmt.excluded.raw,
        },
    )
    res = db.execute(stmt)
    return res.rowcount or 0


def upsert_medication_requests(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = insert(MedicationRequest).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[MedicationRequest.id],
        set_={
            "patient_id": stmt.excluded.patient_id,
            "status": stmt.excluded.status,
            "intent": stmt.excluded.intent,
            "medication_code": stmt.excluded.medication_code,
            "authored_on": stmt.excluded.authored_on,
            "raw": stmt.excluded.raw,
        },
    )
    res = db.execute(stmt)
    return res.rowcount or 0
