from __future__ import annotations

import html
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.utils.email import send_email

REPORT_FILE = Path(os.getenv("LOCAL_REPORT_DIR", "/opt/airflow/reports")) / "daily_summary.txt"
SPARK_JOB = "/opt/pipeline/batch/spark_daily_batch_job.py"

with DAG(
    dag_id="daily_clickstream_batch_processing",
    description="Daily segmentation, top-product, and conversion-rate report",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "clickstream-team", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["clickstream", "spark", "delta", "batch"],
):
    @task
    def run_spark_batch_job() -> str:
        spark_submit = shutil.which("spark-submit")
        if spark_submit is None:
            raise FileNotFoundError("spark-submit is not available in the Airflow image")

        subprocess.run(
            [spark_submit, "--master", os.getenv("SPARK_MASTER", "spark://spark-master:7077"), SPARK_JOB],
            check=True,
        )
        if not REPORT_FILE.exists():
            raise FileNotFoundError(f"Expected report file was not created: {REPORT_FILE}")
        return str(REPORT_FILE)

    @task
    def email_summary(report_file: str) -> None:
        recipient = os.getenv("REPORT_EMAIL_TO", "reports@example.com")
        report_body = Path(report_file).read_text(encoding="utf-8")
        send_email(
            to=recipient,
            subject=f"Daily Clickstream Summary - {datetime.utcnow():%Y-%m-%d}",
            html_content=f"<pre>{html.escape(report_body)}</pre>",
        )

    email_summary(run_spark_batch_job())
