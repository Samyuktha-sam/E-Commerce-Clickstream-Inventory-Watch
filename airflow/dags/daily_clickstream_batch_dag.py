from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "clickstream-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_clickstream_batch_processing",
    default_args=default_args,
    description="Run Spark batch job for daily clickstream analytics",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_spark_batch_job = BashOperator(
        task_id="run_spark_daily_batch_job",
        bash_command="spark-submit /opt/airflow/batch/spark_daily_batch_job.py",
    )

    run_spark_batch_job
