"""
Airflow DAG — daily ClickHouse batch load
Runs after the Spark daily batch job, loading Parquet reports from MinIO
into ClickHouse so the BI dashboard has fresh batch-layer data each morning.

Schedule: daily at 02:00 UTC (after the Spark job which runs at 01:00 UTC)
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner":        "clickstream-team",
    "retries":      2,
    "retry_delay":  timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Helpers used by PythonOperator tasks
# ---------------------------------------------------------------------------

def load_batch_to_clickhouse(**context) -> dict:
    """
    PythonOperator callable.
    Imports batch_to_clickhouse from the batch_layer directory and runs it.
    REPORT_DATE is set to the DAG logical date so backfills work correctly.
    """
    logical_date: datetime = context["logical_date"]
    report_date  = logical_date.date()

    # Make batch_layer importable inside the Airflow container
    sys.path.insert(0, "/opt/airflow/batch_layer")
    import batch_to_clickhouse  # noqa: PLC0415  (late import is intentional)

    summary = batch_to_clickhouse.run(report_date=report_date)
    context["ti"].xcom_push(key="load_summary", value=summary)
    return summary


def verify_clickhouse_rows(**context) -> None:
    """
    PythonOperator callable.
    Connects to ClickHouse and asserts that rows were loaded for today.
    Raises on empty tables to trigger a retry / alert.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/batch_layer")

    from clickhouse_driver import Client  # noqa: PLC0415

    ch = Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        database=os.getenv("CLICKHOUSE_DB", "clickstream"),
    )

    logical_date: datetime = context["logical_date"]
    report_date  = logical_date.date()

    rows = ch.execute(
        "SELECT count() FROM batch_top_products WHERE report_date = %(d)s",
        {"d": report_date},
    )
    count = rows[0][0]
    if count == 0:
        raise ValueError(
            f"Verification failed: batch_top_products has 0 rows for {report_date}"
        )

    print(f"Verification passed: {count} top-product rows for {report_date}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="daily_clickhouse_batch_load",
    default_args=default_args,
    description=(
        "Load daily Spark batch results (top products, user segments) "
        "from MinIO Parquet into ClickHouse serving layer"
    ),
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    catchup=False,
    tags=["serving", "clickhouse", "batch"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — Wait for the Spark batch output to land in MinIO
    #          (simple poll via mc ls; replace with a Sensor for production)
    # -----------------------------------------------------------------------
    wait_for_spark_output = BashOperator(
        task_id="wait_for_spark_output",
        bash_command=(
            "for i in $(seq 1 12); do "
            "  mc alias set local http://${MINIO_ENDPOINT:-minio:9000} "
            "       ${MINIO_ACCESS_KEY:-minioadmin} ${MINIO_SECRET_KEY:-minioadmin} "
            "  && mc ls local/clickstream-lake/reports/top_products/ "
            "  && echo 'Output found' && exit 0; "
            "  echo \"Attempt $i: output not ready yet, waiting 30s...\"; "
            "  sleep 30; "
            "done; "
            "echo 'Timeout waiting for Spark output'; exit 1"
        ),
        retries=1,
    )

    # -----------------------------------------------------------------------
    # Task 2 — Load Parquet → ClickHouse
    # -----------------------------------------------------------------------
    load_to_clickhouse = PythonOperator(
        task_id="load_batch_to_clickhouse",
        python_callable=load_batch_to_clickhouse,
    )

    # -----------------------------------------------------------------------
    # Task 3 — Verify rows landed in ClickHouse
    # -----------------------------------------------------------------------
    verify_load = PythonOperator(
        task_id="verify_clickhouse_rows",
        python_callable=verify_clickhouse_rows,
    )

    # -----------------------------------------------------------------------
    # Task 4 — Refresh ClickHouse materialized views (optional, for SummingMT)
    # -----------------------------------------------------------------------
    optimize_tables = BashOperator(
        task_id="optimize_clickhouse_tables",
        bash_command=(
            "clickhouse-client "
            "--host ${CLICKHOUSE_HOST:-clickhouse} "
            "--query 'OPTIMIZE TABLE clickstream.batch_top_products FINAL; "
            "         OPTIMIZE TABLE clickstream.batch_user_segments FINAL;'"
        ),
        retries=1,
    )

    # Pipeline order
    wait_for_spark_output >> load_to_clickhouse >> verify_load >> optimize_tables


# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta

# default_args = {
#     "owner": "clickstream-team",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="daily_clickstream_batch_processing",
#     default_args=default_args,
#     description="Run Spark batch job for daily clickstream analytics",
#     start_date=datetime(2026, 1, 1),
#     schedule_interval="@daily",
#     catchup=False,
# ) as dag:

#     run_spark_batch_job = BashOperator(
#         task_id="run_spark_daily_batch_job",
#         bash_command="spark-submit /opt/airflow/batch/spark_daily_batch_job.py",
#     )

#     run_spark_batch_job