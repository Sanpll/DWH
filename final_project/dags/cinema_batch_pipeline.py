"""Daily batch pipeline for archived cinema events."""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


def run_script(script: str, **context) -> None:
    env = os.environ.copy()
    dataset = context["params"]["dataset"]
    env["DATASET"] = dataset
    env["EVENTS_TOPIC"] = "cinema.events"
    env["PROFILES_TOPIC"] = "cinema.profile_changes"
    env["ARCHIVE_PREFIX"] = "raw"
    env["CATALOGUE_DATASET"] = dataset
    env["RUN_ID"] = context["run_id"]
    env["DAG_ID"] = context["dag"].dag_id
    env["TASK_ID"] = context["task"].task_id
    env["DATA_INTERVAL_START"] = context["data_interval_start"].isoformat()
    env["DATA_INTERVAL_END"] = context["data_interval_end"].isoformat()
    subprocess.run([sys.executable, f"/opt/airflow/{script}"], check=True, env=env)


with DAG(
    dag_id="cinema_batch_pipeline",
    description="Archive to Spark marts to ClickHouse (fixture-first pipeline)",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    # `airflow dags test` runs without a scheduler and cannot promote
    # UP_FOR_RETRY tasks. The notebook sets this to 0 for local test runs;
    # normal scheduler runs keep one retry by default.
    default_args={"owner": "student", "retries": int(os.getenv("CINEMA_DAG_RETRIES", "1"))},
    params={"dataset": Param("fixture", enum=["fixture", "demo"], type="string")},
    tags=["cinema", "batch"],
) as dag:
    upload_catalogue = PythonOperator(
        task_id="upload_catalogue",
        python_callable=run_script,
        op_kwargs={"script": "scripts/upload_catalogue.py"},
    )
    prepare_and_build_marts = PythonOperator(
        task_id="spark_process_and_validate",
        python_callable=run_script,
        op_kwargs={"script": "jobs/process_archive.py"},
    )
    compare_implementations = PythonOperator(
        task_id="compare_rdd_dataframe",
        python_callable=run_script,
        op_kwargs={"script": "jobs/compare_rdd_dataframe.py"},
    )
    publish = PythonOperator(
        task_id="publish_clickhouse_marts",
        python_callable=run_script,
        op_kwargs={"script": "scripts/publish_batch_marts.py"},
    )

    upload_catalogue >> prepare_and_build_marts >> compare_implementations >> publish
