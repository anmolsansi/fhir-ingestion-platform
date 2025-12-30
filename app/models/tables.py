from __future__ import annotations

from sqlalchemy import String, Date, DateTime, JSON, ForeignKey, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class Patient(Base):
    __tablename__ = "patients"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # FHIR id
    family: Mapped[str | None] = mapped_column(String, nullable=True)
    given: Mapped[str | None] = mapped_column(String, nullable=True)
    gender: Mapped[str | None] = mapped_column(String, nullable=True)
    birth_date: Mapped[str | None] = mapped_column(String, nullable=True)  # keep string for FHIR date flexibility
    raw: Mapped[dict] = mapped_column(JSON, nullable=False)

    observations: Mapped[list["Observation"]] = relationship(back_populates="patient")


class Observation(Base):
    __tablename__ = "observations"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # FHIR id
    patient_id: Mapped[str] = mapped_column(String, ForeignKey("patients.id"), index=True)
    status: Mapped[str | None] = mapped_column(String, nullable=True)

    code: Mapped[str | None] = mapped_column(String, nullable=True)
    code_display: Mapped[str | None] = mapped_column(String, nullable=True)

    effective_datetime: Mapped[str | None] = mapped_column(String, nullable=True)

    value_quantity: Mapped[float | None] = mapped_column(nullable=True)
    value_unit: Mapped[str | None] = mapped_column(String, nullable=True)
    value_string: Mapped[str | None] = mapped_column(Text, nullable=True)

    raw: Mapped[dict] = mapped_column(JSON, nullable=False)

    patient: Mapped[Patient] = relationship(back_populates="observations")


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    resource_type: Mapped[str] = mapped_column(String, index=True)
    started_at: Mapped[str] = mapped_column(String, default=lambda: func.now().cast(String))
    finished_at: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, default="RUNNING")  # RUNNING/SUCCESS/FAILED
    details: Mapped[dict] = mapped_column(JSON, default=dict)


class Checkpoint(Base):
    __tablename__ = "checkpoints"
    resource_type: Mapped[str] = mapped_column(String, primary_key=True)
    last_successful_lastupdated: Mapped[str | None] = mapped_column(String, nullable=True)
    updated_at: Mapped[str] = mapped_column(String, default=lambda: func.now().cast(String))
