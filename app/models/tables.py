from __future__ import annotations

from typing import Optional

from sqlalchemy import Integer, String, Date, DateTime, JSON, ForeignKey, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class Patient(Base):
    __tablename__ = "patients"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # FHIR id
    family: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    given: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    gender: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    birth_date: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # keep string for FHIR date flexibility
    raw: Mapped[dict] = mapped_column(JSON, nullable=False)

    observations: Mapped[list["Observation"]] = relationship(back_populates="patient")


class Observation(Base):
    __tablename__ = "observations"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # FHIR id
    patient_id: Mapped[str] = mapped_column(String, ForeignKey("patients.id"), index=True)
    status: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    code_display: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    effective_datetime: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    value_quantity: Mapped[Optional[float]] = mapped_column(nullable=True)
    value_unit: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    value_string: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    raw: Mapped[dict] = mapped_column(JSON, nullable=False)

    patient: Mapped[Patient] = relationship(back_populates="observations")


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    resource_type: Mapped[str] = mapped_column(String, index=True)
    started_at: Mapped[str] = mapped_column(String, default=lambda: func.now().cast(String))
    finished_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, default="RUNNING")  # RUNNING/SUCCESS/FAILED
    details: Mapped[dict] = mapped_column(JSON, default=dict)


class Checkpoint(Base):
    __tablename__ = "checkpoints"
    resource_type: Mapped[str] = mapped_column(String, primary_key=True)
    last_successful_lastupdated: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updated_at: Mapped[str] = mapped_column(String, default=lambda: func.now().cast(String))


class DeadLetter(Base):
    __tablename__ = "dead_letters"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    resource_type: Mapped[str] = mapped_column(String, index=True)
    resource_id: Mapped[Optional[str]] = mapped_column(String, index=True, nullable=True)
    stage: Mapped[str] = mapped_column(String)
    error: Mapped[str] = mapped_column(String)
    raw: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    created_at: Mapped[str] = mapped_column(String, default=lambda: func.now().cast(String))
class Encounter(Base):
    __tablename__ = "encounters"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    patient_id: Mapped[str] = mapped_column(String, ForeignKey("patients.id"), index=True)
    status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    encounter_class: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    start: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    end: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    raw: Mapped[dict] = mapped_column(JSON, nullable=False)


class Condition(Base):
    __tablename__ = "conditions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    patient_id: Mapped[str] = mapped_column(String, ForeignKey("patients.id"), index=True)
    clinical_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    verification_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    onset: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    recorded_date: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    raw: Mapped[dict] = mapped_column(JSON, nullable=False)


class MedicationRequest(Base):
    __tablename__ = "medication_requests"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    patient_id: Mapped[str] = mapped_column(String, ForeignKey("patients.id"), index=True)
    status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    intent: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    medication_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    authored_on: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    raw: Mapped[dict] = mapped_column(JSON, nullable=False)
