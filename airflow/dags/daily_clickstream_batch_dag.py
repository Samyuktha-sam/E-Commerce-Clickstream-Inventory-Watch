from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 10),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_s3_minio_ingestion',
    default_args=default_args,
    schedule_interval=None, # Trigger manually for testing
    catchup=False
) as dag:

    # This task replaces your "docker compose exec ..." command
    submit_spark_job = SparkSubmitOperator(
        task_id='ingest_s3_to_spark',
        conn_id='spark_local',  # You must create this connection in Airflow UI
        application='/opt/airflow/dags/spark_user_segmentation.py', # Path ON THE SPARK MASTER
        name='S3IngestionTask',
        packages='org.apache.hadoop:hadoop-aws:3.4.2',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.sql.streaming.metricsEnabled': 'false',
            'spark.sql.streaming.ui.enabled': 'false',
            'spark.ui.showConsoleProgress': 'false'
        },
        verbose=True
    )

    submit_spark_job