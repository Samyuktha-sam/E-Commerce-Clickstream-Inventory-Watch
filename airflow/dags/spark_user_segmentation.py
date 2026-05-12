# from pyspark.sql import SparkSession
# import os


# def create_spark_session():
#     # We define the packages here, but it's better to pass them via spark-submit
#     return (
#         SparkSession.builder.appName("S3IngestionTestAirflow")
#         # .config(
#         #     "spark.jars.packages",
#         #     "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
#         # )
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .getOrCreate()
#     )


# def main():
#     spark = create_spark_session()

#     # # --- S3A CONFIGURATION ---
#     # # In a real prod environment, use Instance Profiles or Secret Manager.
#     # # For testing, you can set these as environment variables or hardcode them here.
#     # hadoop_conf = spark._jsc.hadoopConfiguration()
#     # hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY", "your_access_key"))
#     # hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY", "your_secret_key"))

#     # # If using MinIO, uncomment the lines below:
#     # # hadoop_conf.set("fs.s3a.endpoint", "http://broker:9000") # Use your endpoint
#     # # hadoop_conf.set("fs.s3a.path.style.access", "true")
#     # # hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

#     s3_path = "s3a://clickstream-lake/raw-events/event_date=2026-05-10"

#     try:
#         print(f"--- Attempting to read from: {s3_path} ---")

#         # 1. Read the data
#         df = spark.read.parquet(s3_path)

#         # 2. Basic Validation
#         print("--- Schema Discovery ---")
#         df.printSchema()

#         row_count = df.count()
#         print(f"--- Data Ingested Successfully: {row_count} rows found ---")

#         print("--- Sample Data ---")
#         df.show(5, truncate=False)

#     except Exception as e:
#         print(f"!!! INGESTION ERROR !!!\n{e}")
#     finally:
#         spark.stop()


# if __name__ == "__main__":
#     main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import sys
import os
from datetime import datetime


def main(date_str):
    # Create local reports directory (important for Airflow)
    local_reports_dir = "/opt/airflow/reports"
    os.makedirs(local_reports_dir, exist_ok=True)

    spark = (
        SparkSession.builder.appName(f"DailyUserSegmentation_{date_str}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    try:
        # Read raw data
        input_path = f"s3a://clickstream-lake/raw/date={date_str}/"
        df = spark.read.parquet(input_path)

        # ====================== User Segmentation ======================
        user_stats = (
            df.groupBy("user_id")
            .agg(
                count(when(col("event_type") == "view", 1)).alias("views"),
                count(when(col("event_type") == "purchase", 1)).alias("purchases"),
            )
            .withColumn(
                "segment",
                when(col("purchases") > 0, "Buyer").otherwise("Window Shopper"),
            )
        )

        user_stats.write.mode("overwrite").parquet(
            f"s3a://clickstream-lake/processed/segments/date={date_str}/"
        )

        # ====================== Top 5 Products ======================
        top_products = (
            df.filter(col("event_type") == "view")
            .groupBy("product_id")
            .count()
            .withColumnRenamed("count", "views")
            .orderBy(col("views").desc())
            .limit(5)
        )

        # ====================== Category Conversion Rates ======================
        category_stats = (
            df.groupBy("category")
            .agg(
                count(when(col("event_type") == "view", 1)).alias("views"),
                count(when(col("event_type") == "purchase", 1)).alias("purchases"),
            )
            .withColumn(
                "conversion_rate",
                when(
                    col("views") > 0, (col("purchases") / col("views")).cast("float")
                ).otherwise(0.0),
            )
            .orderBy(col("conversion_rate").desc())
        )

        # ====================== Save to MinIO (Partitioned) ======================
        top_products.coalesce(1).write.mode("overwrite").csv(
            f"s3a://clickstream-lake/reports/top_products/date={date_str}/", header=True
        )

        category_stats.coalesce(1).write.mode("overwrite").csv(
            f"s3a://clickstream-lake/reports/category_conversion/date={date_str}/",
            header=True,
        )

        # ====================== Save Local Copies for Airflow ======================
        top_file = f"{local_reports_dir}/top_products_{date_str}.csv"
        conv_file = f"{local_reports_dir}/conversion_{date_str}.csv"
        summary_file = f"{local_reports_dir}/summary_{date_str}.txt"

        top_products.toPandas().to_csv(top_file, index=False)
        category_stats.toPandas().to_csv(conv_file, index=False)

        print(f"✅ Reports saved locally:\n   {top_file}\n   {conv_file}")

    except Exception as e:
        print(f"❌ Error processing date {date_str}: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        # Fallback for local testing
        date_str = datetime.today().strftime("%Y-%m-%d")
        print(f"Running for today: {date_str}")
        main(date_str)
