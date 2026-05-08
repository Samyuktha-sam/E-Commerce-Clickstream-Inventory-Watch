"""
Spark Structured Streaming app to process clickstream events from Kafka and detect flash sales.
"""

import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType


def main() -> None:
    """Process clickstream events from Kafka and detect flash sales."""
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "clickstream-events")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    lake_root = os.getenv("MINIO_LAKE_PATH", "s3a://clickstream-lake")

    spark = (
        SparkSession.builder.appName("ClickstreamFlashSaleDetector")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
        )
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    decoded_df = (
        raw_stream.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp").cast("timestamp")))
        .withColumn("event_date", F.to_date("event_time"))
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
    )

    windowed_stats = decoded_df.groupBy(
        F.window("event_time", "5 minutes", "1 minute"), "product_id"
    ).agg(
        F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchase_count"),
        F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias(
            "add_to_cart_count"
        ),
        F.sum((F.col("event_type") == "view").cast("int")).alias("view_count"),
    )

    alerts_df = windowed_stats.filter(
        (F.col("view_count") > 5) & (F.col("purchase_count") < 2)
    ).select(
        "window.start",
        "window.end",
        "product_id",
        "view_count",
        "purchase_count",
        F.lit("Flash Sale Recommended").alias("action"),
    )

    # 1. Start the Console Query
    query = (
        alerts_df.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 2. Start the Parquet/Data Lake Query
    raw_events_query = (
        decoded_df.writeStream.format("parquet")
        .option("path", f"{lake_root}/raw-events")
        .option("checkpointLocation", f"{lake_root}/_checkpoints/raw-events")
        .outputMode("append")
        .partitionBy("event_date")
        .trigger(processingTime="6 seconds")
        .start()
    )

    print("Streaming started... Waiting for data from Kafka.")

    # 3. NOW wait for them to finish
    # This prevents the script from exiting while the streams are running
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
