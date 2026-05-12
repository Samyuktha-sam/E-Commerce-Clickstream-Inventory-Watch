"""
Daily User Segmentation Pipeline
Orchestrates Spark jobs to analyze clickstream data and send reports via email.
"""

from datetime import datetime
from typing import TypedDict, List
import io
import os

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Type Definitions for Type Safety ---


class PartitionInfo(TypedDict):
    """Metadata about the raw data partition."""

    exists: bool
    path: str
    date: str


class ReportSummary(TypedDict):
    """Container for report summaries to be emailed."""

    top_5: str
    conversion: str
    date: str


# --- Spark Utility ---

# def get_spark_session(app_name: str) -> SparkSession:
#     """
#     Creates and returns a Spark session configured for Minio (S3A).
#     Ensures S3A filesystem implementation and credentials are set.
#     """
#     return (
#         SparkSession.builder.appName(app_name)
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         # Ensure we have the necessary packages for S3A
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
#         .getOrCreate()
#     )

# --- DAG Definition ---


@dag(
    dag_id="daily_user_segmentation_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["spark", "user_segmentation", "reports", "airflow3"],
)
def daily_user_segmentation_pipeline():
    """
    Pipeline that:
    1. Checks for data in Minio for the logical date.
    2. Runs 3 parallel Spark analysis jobs.
    3. Saves results in JSON and TXT formats to Minio.
    4. Sends a consolidated email report.
    """

    @task
    def check_minio_data(ds=None) -> PartitionInfo:
        """
        Check if raw-events exist for the current day's partition.
        Returns partition metadata or skips the DAG if no data is found.
        """
        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        bucket = "clickstream-lake"
        prefix = f"raw-events/event_date={ds}/"

        print(f"Checking for data in bucket '{bucket}' with prefix '{prefix}'...")
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        exists = any(True for _ in objects)

        if not exists:
            print(f"⚠️ No data found for date: {ds}. Skipping downstream tasks.")
            raise AirflowSkipException(f"No data for {ds}")

        return {"exists": True, "path": f"s3a://{bucket}/{prefix}", "date": ds}

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={"spark.remote": "sc://spark-master:15002"},
    )
    def user_segmentation_task(partition: PartitionInfo, spark=None) -> str:
        """
        Spark task: Categorize users into 'Buyer' or 'Window Shopper'.
        Saves full JSON results and a summary TXT to the shared partition.
        """
        ds = partition["date"]
        try:
            print(f"Reading data from {partition['path']}...")
            df = spark.read.parquet(partition["path"])
            df.show(5, truncate=False)  # Basic validation and sample data display

            # Segmentation logic: Buyers have at least one 'purchase' event.
            segments = (
                df.groupBy("user_id")
                .agg(
                    F.count(F.when(F.col("event_type") == "purchase", 1)).alias(
                        "purchases"
                    ),
                    F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
                )
                .withColumn(
                    "segment",
                    F.when(F.col("purchases") > 0, "Buyer").otherwise("Window Shopper"),
                )
            )

            output_path = f"s3a://clickstream-lake/processed/segments/date={ds}"
            print(f"Saving JSON results to {output_path}/json...")
            segments.write.mode("overwrite").json(f"{output_path}/json")

            # Save summary TXT using Spark
            summary = segments.groupBy("segment").count()
            summary_txt = summary.select(
                F.concat(F.col("segment"), F.lit(": "), F.col("count")).alias("summary")
            )
            print(f"Saving TXT summary to {output_path}/txt...")
            summary_txt.coalesce(1).write.mode("overwrite").text(f"{output_path}/txt")

            return output_path
        finally:
            spark.stop()

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={"spark.remote": "sc://spark-master:15002"},
    )
    def top_products_analysis_task(partition: PartitionInfo, spark=None) -> str:
        """
        Spark task: Identify the top 5 most viewed products.
        Saves JSON and generates a formatted TXT summary.
        """
        ds = partition["date"]
        try:
            df = spark.read.parquet(partition["path"])

            top_5 = (
                df.filter(F.col("event_type") == "view")
                .groupBy("product_id")
                .count()
                .orderBy(F.col("count").desc())
                .limit(5)
            )

            output_path = f"s3a://clickstream-lake/reports/top_products/date={ds}"
            print(f"Saving JSON results to {output_path}/json...")
            top_5.write.mode("overwrite").json(f"{output_path}/json")

            # Generate formatted TXT summary for the email
            rows = top_5.collect()
            txt_lines = ["Top 5 Most Viewed Products:", "-" * 30]
            for row in rows:
                txt_lines.append(
                    f"Product: {row['product_id']} | Views: {row['count']}"
                )
            txt_content = "\n".join(txt_lines)

            # Save TXT summary to Minio using Spark (as requested)
            txt_df = spark.createDataFrame([(txt_content,)], ["content"])
            txt_df.coalesce(1).write.mode("overwrite").text(f"{output_path}/txt")

            return txt_content
        finally:
            spark.stop()

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={"spark.remote": "sc://spark-master:15002"},
    )
    def analytic_report_task(partition: PartitionInfo, spark=None) -> str:
        """
        Spark task: Calculate conversion rates per product category.
        Saves JSON and generates a formatted TXT report.
        """
        ds = partition["date"]
        try:
            df = spark.read.parquet(partition["path"])

            report = (
                df.groupBy("category")
                .agg(
                    F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
                    F.count(F.when(F.col("event_type") == "purchase", 1)).alias(
                        "purchases"
                    ),
                )
                .withColumn(
                    "conversion_rate",
                    F.when(
                        F.col("views") > 0,
                        (F.col("purchases") / F.col("views")).cast("float"),
                    ).otherwise(0.0),
                )
                .orderBy(F.col("conversion_rate").desc())
            )

            output_path = f"s3a://clickstream-lake/reports/conversion/date={ds}"
            print(f"Saving JSON results to {output_path}/json...")
            report.write.mode("overwrite").json(f"{output_path}/json")

            # Generate formatted TXT summary
            rows = report.collect()
            txt_lines = ["Category Conversion Rates:", "-" * 30]
            for row in rows:
                category = row["category"] or "Other"
                txt_lines.append(
                    f"Category: {category:12} | CR: {row['conversion_rate']:6.2%} (Views: {row['views']}, Purchases: {row['purchases']})"
                )
            txt_content = "\n".join(txt_lines)

            # Save TXT summary to Minio using Spark
            txt_df = spark.createDataFrame([(txt_content,)], ["content"])
            txt_df.coalesce(1).write.mode("overwrite").text(f"{output_path}/txt")

            return txt_content
        finally:
            spark.stop()

    @task
    def send_reports_email(top_5: str, conversion: str, partition: PartitionInfo):
        """
        Sends the consolidated analytical report via email.
        """
        ds = partition["date"]

        email_body = f"""
        <h3>Daily Analytics Report - {ds}</h3>
        <p>The daily processing pipeline has completed successfully.</p>
        
        <p><b>1. Top 5 Most Viewed Products:</b></p>
        <pre style="background: #f4f4f4; padding: 10px; border: 1px solid #ddd;">{top_5}</pre>
        
        <p><b>2. Category Conversion Rates:</b></p>
        <pre style="background: #f4f4f4; padding: 10px; border: 1px solid #ddd;">{conversion}</pre>
        
        <p><i>Full datasets available in Minio under: s3a://clickstream-lake/reports/date={ds}/</i></p>
        """

        print(f"Sending email report for {ds}...")
        print(email_body)

    # --- Pipeline Flow ---

    # 1. Check for data
    partition = check_minio_data()

    # 2. Parallel Spark processing (3 tasks)
    seg = user_segmentation_task(partition)
    top = top_products_analysis_task(partition)
    conv = analytic_report_task(partition)

    # 3. Consolidate and Email (Depends on parallel processing tasks)
    send_reports_email(top, conv, partition)


# Instantiate the DAG
pipeline = daily_user_segmentation_pipeline()
