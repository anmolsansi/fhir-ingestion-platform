from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator

API_BASE_URL = os.getenv("FHIR_API_BASE_URL", "http://api:8000").rstrip("/")


def _post(endpoint: str, params: dict[str, object | None]) -> dict:
    clean_params = {key: value for key, value in params.items() if value is not None}
    url = f"{API_BASE_URL}{endpoint}?{urlencode(clean_params)}"
    req = Request(url, method="POST")
    with urlopen(req, timeout=300) as resp:
        body = resp.read().decode("utf-8")
    return {"status_code": resp.status, "body": body}


def ingest_patients_task(**context):
    ti = context["ti"]
    dag_run = context.get("dag_run")
    return _post(
        "/ingest/patients",
        {
            "job": f"{ti.dag_id}.{ti.task_id}",
            "trigger": "airflow",
            "attempt": ti.try_number,
            "max_attempts": (ti.task.retries or 0) + 1,
            "run_group": dag_run.run_id if dag_run else None,
            "airflow_dag_id": ti.dag_id,
            "airflow_task_id": ti.task_id,
            "airflow_run_id": dag_run.run_id if dag_run else None,
        },
    )


def ingest_observations_task(**context):
    ti = context["ti"]
    dag_run = context.get("dag_run")
    return _post(
        "/ingest/observations",
        {
            "job": f"{ti.dag_id}.{ti.task_id}",
            "trigger": "airflow",
            "attempt": ti.try_number,
            "max_attempts": (ti.task.retries or 0) + 1,
            "run_group": dag_run.run_id if dag_run else None,
            "airflow_dag_id": ti.dag_id,
            "airflow_task_id": ti.task_id,
            "airflow_run_id": dag_run.run_id if dag_run else None,
        },
    )


def ingest_observations_backfill_task(**context):
    ti = context["ti"]
    dag_run = context.get("dag_run")
    return _post(
        "/ingest/observations/backfill",
        {
            "job": f"{ti.dag_id}.{ti.task_id}",
            "trigger": "airflow",
            "attempt": ti.try_number,
            "max_attempts": (ti.task.retries or 0) + 1,
            "run_group": dag_run.run_id if dag_run else None,
            "airflow_dag_id": ti.dag_id,
            "airflow_task_id": ti.task_id,
            "airflow_run_id": dag_run.run_id if dag_run else None,
        },
    )


def ingest_encounters_task(**context):
    ti = context["ti"]
    dag_run = context.get("dag_run")
    return _post(
        "/ingest/encounters",
        {
            "job": f"{ti.dag_id}.{ti.task_id}",
            "trigger": "airflow",
            "attempt": ti.try_number,
            "max_attempts": (ti.task.retries or 0) + 1,
            "run_group": dag_run.run_id if dag_run else None,
            "airflow_dag_id": ti.dag_id,
            "airflow_task_id": ti.task_id,
            "airflow_run_id": dag_run.run_id if dag_run else None,
        },
    )


def ingest_conditions_task(**context):
    ti = context["ti"]
    dag_run = context.get("dag_run")
    return _post(
        "/ingest/conditions",
        {
            "job": f"{ti.dag_id}.{ti.task_id}",
            "trigger": "airflow",
            "attempt": ti.try_number,
            "max_attempts": (ti.task.retries or 0) + 1,
            "run_group": dag_run.run_id if dag_run else None,
            "airflow_dag_id": ti.dag_id,
            "airflow_task_id": ti.task_id,
            "airflow_run_id": dag_run.run_id if dag_run else None,
        },
    )


with DAG(
    dag_id="fhir_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2},
    tags=["fhir", "ingestion"],
) as dag:
    ingest_patients = PythonOperator(
        task_id="ingest_patients",
        python_callable=ingest_patients_task,
    )
    ingest_observations = PythonOperator(
        task_id="ingest_observations",
        python_callable=ingest_observations_task,
    )
    ingest_encounters = PythonOperator(
        task_id="ingest_encounters",
        python_callable=ingest_encounters_task,
    )
    ingest_conditions = PythonOperator(
        task_id="ingest_conditions",
        python_callable=ingest_conditions_task,
    )
    ingest_observations_backfill = PythonOperator(
        task_id="ingest_observations_backfill",
        python_callable=ingest_observations_backfill_task,
        trigger_rule="all_done",
    )

    ingest_patients >> ingest_observations >> ingest_encounters >> ingest_conditions >> ingest_observations_backfill
