"""Optional Airflow wrapper. The supported runner without Airflow is run_pipeline.py."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

from uddp.pipeline import run_pipeline, scheduled_batch_id


def run_scheduled_batch(**context):
    """Retries of this task reuse the logical date, so they reload the same batch id."""
    logical_date = context["data_interval_start"]
    return run_pipeline(batch_id=scheduled_batch_id(logical_date))


with DAG(
    dag_id="jharkhand_mini_uddp",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["jharkhand", "uddp"],
) as dag:
    PythonOperator(
        task_id="build_district_mart",
        python_callable=run_scheduled_batch,
    )
