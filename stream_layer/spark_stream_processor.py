import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("pyspark").setLevel(logging.WARN)
logging.getLogger("py4j").setLevel(logging.WARN)


@dataclass(frozen=True)
class AppConfig:
    """Centralized application configuration."""

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    lake_root: str = os.getenv("MINIO_LAKE_PATH", "s3a://clickstream-lake")
    spark_master: str = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    spark_checkpoint: str = os.getenv(
        "SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/clickstream-app"
    )
    alerts_topic: str = os.getenv("ALERTS_TOPIC", "alerts-notifications")

    # Pre-defined schema for the clickstream
    CLICKSTREAM_SCHEMA: StructType = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )


# --- Logic Modules ---


def get_spark_session(config: AppConfig) -> SparkSession:
    """Builds and returns a SparkSession with optimized S3A/Kafka settings."""
    return (
        SparkSession.builder.appName("ClickstreamFlashSaleDetector")
        .master(config.spark_master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
        )
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.streaming.checkpointLocation", config.spark_checkpoint)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def transform_raw_stream(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """Parses JSON, casts timestamps, and applies watermarking."""
    return (
        raw_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
    )


def detect_flash_sales(df: DataFrame) -> DataFrame:
    """Aggregates windowed events and filters for high-view/low-purchase items."""
    windowed_stats = df.groupBy(
        F.window("event_time", "5 minutes", "1 minute"), "product_id"
    ).agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.count(F.when(F.col("event_type") == "view", 1)).alias("view_count"),
    )

    return windowed_stats.filter(
        (F.col("view_count") > 5) & (F.col("purchase_count") < 2)
    ).select(
        "window.start",
        "window.end",
        "product_id",
        "view_count",
        "purchase_count",
        F.lit("Flash Sale Recommended").alias("action"),
    )


def main() -> None:
    config = AppConfig()
    spark = get_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Source
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # 2. Transform
        decoded_df = transform_raw_stream(raw_stream, config.CLICKSTREAM_SCHEMA)
        alerts_df = detect_flash_sales(decoded_df)

        # 3. Sinks
        # Sink A: Publish alerts to Kafka only when the alert condition is met.
        alerts_query = (
            alerts_df.selectExpr("to_json(struct(*)) AS value")
            .writeStream.outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("topic", config.alerts_topic)
            .option(
                "checkpointLocation",
                f"{config.spark_checkpoint}/alerts-notifications",
            )
            .trigger(processingTime="30 seconds")
            .start()
        )

        # console_query = (
        #     alerts_df.writeStream.outputMode("update")
        #     .format("console")
        #     .option("truncate", "false")
        #     .trigger(processingTime="30 seconds")
        #     .start()
        # )

        # Sink B: Parquet Data Lake
        lake_query = (
            decoded_df.writeStream.format("parquet")
            .option("path", f"{config.lake_root}/raw-events")
            .option("checkpointLocation", f"{config.lake_root}/_checkpoints/raw-events")
            .outputMode("append")
            .partitionBy("event_date")
            .trigger(processingTime="60 seconds")
            .start()
        )

        logger.info(
            "Streaming queries started. Monitoring Kafka topic: %s", config.kafka_topic
        )
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error("Error in streaming application: %s", str(e))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
