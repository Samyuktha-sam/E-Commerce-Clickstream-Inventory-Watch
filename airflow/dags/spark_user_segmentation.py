
from pyspark.sql import SparkSession
import os


def create_spark_session():
    # We define the packages here, but it's better to pass them via spark-submit
    return (
        SparkSession.builder.appName("S3IngestionTest")
        # .config(
        #     "spark.jars.packages",
        #     "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
        # )
        # .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000" )
        # .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        # .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        # .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    # # --- S3A CONFIGURATION ---
    # # In a real prod environment, use Instance Profiles or Secret Manager.
    # # For testing, you can set these as environment variables or hardcode them here.
    # hadoop_conf = spark._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY", "your_access_key"))
    # hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY", "your_secret_key"))

    # # If using MinIO, uncomment the lines below:
    # # hadoop_conf.set("fs.s3a.endpoint", "http://broker:9000") # Use your endpoint
    # # hadoop_conf.set("fs.s3a.path.style.access", "true")
    # # hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    s3_path = "s3a://clickstream-lake/raw-events/event_date=2026-05-10"

    try:
        print(f"--- Attempting to read from: {s3_path} ---")

        # 1. Read the data
        df = spark.read.parquet(s3_path)

        # 2. Basic Validation
        print("--- Schema Discovery ---")
        df.printSchema()

        row_count = df.count()
        print(f"--- Data Ingested Successfully: {row_count} rows found ---")

        print("--- Sample Data ---")
        df.show(5, truncate=False)

    except Exception as e:
        print(f"!!! INGESTION ERROR !!!\n{e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()