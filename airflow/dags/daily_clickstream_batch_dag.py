from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "clickstream-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_clickstream_batch_processing",
    default_args=default_args,
    description="Run Spark 4.1.1 batch job for daily clickstream analytics from S3",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["clickstream", "spark", "s3", "batch"],
) as dag:

    run_spark_batch_job = BashOperator(
        task_id="run_spark_daily_batch_job",
        bash_command=(
            "spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.4.1 "
            "/opt/airflow/batch/spark_daily_batch_job.py"
        ),
    )

