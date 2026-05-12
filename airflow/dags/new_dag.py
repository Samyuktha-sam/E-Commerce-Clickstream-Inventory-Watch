"""
Airflow DAG for daily batch processing using Airflow SDK (TaskFlow API).
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from daily_batch_python import daily_batch_processing

# Define default arguments
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="daily_batch_python_processing",
    default_args=DEFAULT_ARGS,
    description="Daily batch processing with pure Python using Airflow SDK",
    schedule="0 2 * * *",  # 2 AM daily
    start_date=datetime(2026, 5, 11),
    catchup=False,
    tags=["batch", "python", "s3"],
)
def daily_batch_dag():

    @task(task_id="daily_batch_processing")
    def run_batch():
        """
        Wraps the imported logic into a TaskFlow task.
        """
        return daily_batch_processing()

    # Calling the function creates the task instance and sets dependencies
    run_batch()


# Instantiate the DAG
daily_batch_dag()
