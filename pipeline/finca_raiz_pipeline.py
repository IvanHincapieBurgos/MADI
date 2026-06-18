"""Airflow DAG.
Orchestrates the full Medallion flow, strictly sequential::
    extract  ->  pyspark_transform  ->  dbt_run  ->  dbt_test

* extract            : run the Apify scraper, save raw JSON (Bronze), and PUSH the file path to XCom.
* pyspark_transform  : PULL that path from XCom and pass it to the Spark job (Silver Parquet + load to staging.stg_listings).
* dbt_run / dbt_test : build & test the Star Schema + ideal-housing mart, using the ISOLATED dbt venv ($DBT_BIN).
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --- Make project packages (src.*, etl.*) importable -----------------
# PYTHONPATH=/opt/airflow is set in the image, but be defensive.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

DBT_PROJECT_DIR = os.path.join(AIRFLOW_HOME, "dbt")

_log = logging.getLogger("airflow.task")

def _build_dbt_vars_arg() -> str:
    '''
    Build the dbt --vars argument from config.yaml at parse time.
    '''
    try:
        from src.utils.config_loader import load_config

        filters = load_config()["filters"]
        payload = json.dumps(
            {
                "filter_bedrooms": int(filters["bedrooms"]),
                "filter_allows_pets": bool(filters["allows_pets"]),
                "filter_max_price": int(filters["max_price"]),
            }
        )
        return f"--vars '{payload}'"
    except Exception as exc:  # noqa: BLE001 - never break DAG parsing
        _log.warning("Could not build dbt vars from config (%s); using defaults.", exc)
        return ""


DBT_VARS_ARG = _build_dbt_vars_arg()

def on_failure(context: dict) -> None:

    ti = context.get("task_instance")
    exc = context.get("exception")
    _log.error("=" * 60)
    _log.error("TASK FAILED")
    if ti is not None:
        _log.error("  dag=%s task=%s try=%s/%s", ti.dag_id, ti.task_id, ti.try_number, ti.max_tries)
        _log.error("  log_url=%s", getattr(ti, "log_url", "n/a"))
    if exc is not None:
        _log.error("  exception=%r", exc)
    _log.error("=" * 60)

def extract_callable(**context) -> str:
    '''
    Bronze: run Apify (or the fixture) and return the raw JSON path.
    '''
    params = context.get("params", {})
    if params.get("use_sample_fixture"):
        from src.utils.config_loader import PROJECT_ROOT

        path = str(PROJECT_ROOT / "tests" / "fixtures" / "sample_raw_finca_raiz.json")
        _log.info("use_sample_fixture=True -> skipping Apify, using %s", path)
        return path

    from etl.extract.extract_finca_raiz import run_extraction

    path = run_extraction()
    _log.info("Extraction produced: %s", path)
    return path  # auto-pushed to XCom (key: return_value)


def transform_callable(raw_path: str, **context) -> dict:
    '''
    Silver: pull the raw path (as an argument) and run the Spark job.
    '''
    if not raw_path:
        raise ValueError("No raw_path received from the extract task via XCom.")
    _log.info("Transform received raw_path from XCom: %s", raw_path)

    from etl.transform.transform_finca_raiz import run_transform

    summary = run_transform(input_path=raw_path)
    _log.info("Transform summary: %s", summary)
    return summary

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    # Email placeholders — flip the *_on_* flags to True once SMTP is set.
    "email": ["data-alerts@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="madi_finca_raiz_pipeline",
    description="Fincaraíz rentals: Apify -> PySpark -> Postgres -> dbt star schema",
    default_args=default_args,
    schedule="@daily",          # paused at creation; set to None for manual-only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["madi", "finca-raiz", "medallion", "pyspark", "dbt"],
    doc_md=__doc__,
    params={
        "use_sample_fixture": Param(
            False,
            type="boolean",
            title="Use sample fixture",
            description="Skip Apify and run end-to-end with the bundled sample data.",
        ),
    },
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_callable,
        doc_md="Run the Apify scraper (or fixture) and push the raw JSON path to XCom.",
    )

    transform_task = PythonOperator(
        task_id="pyspark_transform",
        python_callable=transform_callable,
        # Pull the path dynamically from XCom and pass it as an argument.
        op_kwargs={"raw_path": "{{ ti.xcom_pull(task_ids='extract') }}"},
        doc_md="PySpark clean/flatten/derive -> Parquet + load staging.stg_listings.",
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -euo pipefail\n"
            f'cd "{DBT_PROJECT_DIR}"\n'
            'echo "Using dbt at: $DBT_BIN"\n'
            f'"$DBT_BIN" run --profiles-dir . --project-dir . --target dev {DBT_VARS_ARG}\n'
        ),
        doc_md="Build the Star Schema + ideal-housing mart with the isolated dbt venv.",
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "set -euo pipefail\n"
            f'cd "{DBT_PROJECT_DIR}"\n'
            f'"$DBT_BIN" test --profiles-dir . --project-dir . --target dev {DBT_VARS_ARG}\n'
        ),
        doc_md="Run all dbt data tests (uniqueness, relationships, filter assertions).",
    )

    # Strict sequential dependency chain.
    extract_task >> transform_task >> dbt_run_task >> dbt_test_task