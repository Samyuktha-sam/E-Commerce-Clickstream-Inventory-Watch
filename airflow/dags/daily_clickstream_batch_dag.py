# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from datetime import datetime, timedelta

# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2026, 5, 10),
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     "spark_s3_minio_ingestion",
#     default_args=default_args,
#     schedule_interval=None,  # Trigger manually for testing
#     catchup=False,
# ) as dag:

#     # This task replaces your "docker compose exec ..." command
#     submit_spark_job = SparkSubmitOperator(
#         task_id="ingest_s3_to_spark",
#         conn_id="spark_local",  # You must create this connection in Airflow UI
#         application="/opt/airflow/dags/spark_user_segmentation.py",  # Path ON THE SPARK MASTER
#         name="S3IngestionTask",
#         packages="org.apache.hadoop:hadoop-aws:3.4.2",
#         conf={
#             "spark.master": "spark://spark-master:7077",
#             "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
#             "spark.hadoop.fs.s3a.access.key": "minioadmin",
#             "spark.hadoop.fs.s3a.secret.key": "minioadmin",
#             "spark.hadoop.fs.s3a.path.style.access": "true",
#             "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#             "spark.sql.streaming.metricsEnabled": "false",
#             "spark.sql.streaming.ui.enabled": "false",
#             "spark.ui.showConsoleProgress": "false",
#         },
#         verbose=True,
#     )

#     submit_spark_job

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.standard.operators.python import PythonOperator

# import pandas as pd
# from io import BytesIO
# import os
# import boto3

# # ====================== MinIO S3 Configuration ======================
# S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
# S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
# S3_BUCKET = os.getenv("MINIO_BUCKET", "clickstream-lake")

# INPUT_PREFIX_TEMPLATE = os.getenv("MINIO_INPUT_PREFIX", "raw-events/event_date={ds}")


# def get_s3_client():
#     return boto3.client(
#         "s3",
#         endpoint_url=S3_ENDPOINT,
#         aws_access_key_id=S3_ACCESS_KEY,
#         aws_secret_access_key=S3_SECRET_KEY,
#         use_ssl=False,
#     )


# def read_parquet_objects_from_s3(bucket: str, prefix: str, client=None):
#     if client is None:
#         client = get_s3_client()

#     dataframes = []
#     paginator = client.get_paginator("list_objects_v2")

#     for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
#         if "Contents" not in page:
#             continue
#         for obj in page["Contents"]:
#             key = obj["Key"]
#             if key.endswith(".parquet"):
#                 response = client.get_object(Bucket=bucket, Key=key)
#                 dataframes.append(pd.read_parquet(BytesIO(response["Body"].read())))

#     return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()


# def check_s3_data_exists(ds, **_context):
#     """Check whether input data exists for the run date."""
#     client = get_s3_client()
#     prefix = INPUT_PREFIX_TEMPLATE.format(ds=ds)

#     paginator = client.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
#         if page.get("KeyCount", 0) > 0:
#             return True

#     raise ValueError(f"No input data found in s3://{S3_BUCKET}/{prefix}")


# def generate_summary(ds, **_context):
#     """Generate a simple summary report for the run date."""
#     output_path = f"/opt/airflow/reports/summary_{ds}.txt"
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)

#     input_prefix = INPUT_PREFIX_TEMPLATE.format(ds=ds)
#     df = read_parquet_objects_from_s3(S3_BUCKET, input_prefix)

#     summary_text = f"Daily Clickstream Summary for {ds}\nRecords: {len(df)}\n"

#     with open(output_path, "w", encoding="utf-8") as f:
#         f.write(summary_text)

#     return output_path


# # ========================== DAG Definition ==========================
# default_args = {
#     "owner": "data_team",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="daily_user_segmentation",
#     default_args=default_args,
#     schedule="0 2 * * *",
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     tags=["clickstream", "segmentation"],
# ) as dag:

#     check_data = PythonOperator(
#         task_id="check_data_exists",
#         python_callable=check_s3_data_exists,
#     )

#     process_data = SparkSubmitOperator(
#         task_id="spark_user_segmentation",
#         application="/opt/airflow/dags/spark_user_segmentation.py",
#         conn_id="spark_client",
#         application_args=["{{ ds }}"],
#         packages=(
#             "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
#             "org.apache.hadoop:hadoop-aws:3.3.4,"
#             "com.amazonaws:aws-java-sdk-bundle:1.12.262"
#         ),
#         conf={
#             "spark.submit.deployMode": "client",
#             "spark.master": "spark://spark-master:7077",
#             "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
#             "spark.hadoop.fs.s3a.access.key": "minioadmin",
#             "spark.hadoop.fs.s3a.secret.key": "minioadmin",
#             "spark.hadoop.fs.s3a.path.style.access": "true",
#             "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#         },
#         verbose=True,
#     )

#     create_summary = PythonOperator(
#         task_id="create_summary_report",
#         python_callable=generate_summary,
#     )

#     # Task Dependencies
#     check_data >> process_data >> create_summary

from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="hello_world_sdk",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def hello_world_dag():
    
    @task
    def say_hello():
        print("Hello World from Airflow 3 SDK!")
        return "Success"

    say_hello()

hello_world_dag()