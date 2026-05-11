"""
Airflow DAG for daily batch processing using pure Python (no Spark).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from daily_batch_python import daily_batch_processing

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 5, 10),
}

dag = DAG(
    "daily_batch_python_processing",
    default_args=default_args,
    description="Daily batch processing with pure Python (no Spark)",
    schedule="0 2 * * *",  # 2 AM daily
    catchup=False,
    tags=["batch", "python", "s3"],
)

with dag:
    run_batch = PythonOperator(
        task_id="daily_batch_processing",
        python_callable=daily_batch_processing,
    )

    run_batch
