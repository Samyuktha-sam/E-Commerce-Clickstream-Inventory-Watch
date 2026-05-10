import logging
import os
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AppConfig:
    """Configuration specifically for the Data Lake Storage Layer."""

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    lake_root: str = os.getenv("MINIO_LAKE_PATH", "s3a://clickstream-lake")
    spark_master: str = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

    CLICKSTREAM_SCHEMA: StructType = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )


def get_spark_session(config: AppConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("StorageLayer-RawEventArchiver")
        .master(config.spark_master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
        )
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.sql.shuffle.partitions", "2"
        )  # Low shuffle needed for direct write
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def transform_for_lake(raw_df: DataFrame, schema: StructType) -> DataFrame:
    return (
        raw_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .filter(F.col("event_time").isNotNull())
    )


def main() -> None:
    config = AppConfig()
    spark = get_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .load()
        )

        archival_df = transform_for_lake(raw_stream, config.CLICKSTREAM_SCHEMA)

        # # Console output for debugging
        # console_query = (
        #     archival_df.writeStream.format("console")
        #     .option("truncate", "false")
        #     .outputMode("append")
        #     .start()
        # )

        # Sink: Parquet Data Lake on S3/MinIO
        # Partitioning by date is critical for query performance later
        query = (
            archival_df.writeStream.format("parquet")
            .option("path", f"{config.lake_root}/raw-events")
            .option(
                "checkpointLocation",
                f"{config.lake_root}/_checkpoints/raw-events-archiver",
            )
            .outputMode("append")
            .partitionBy("event_date")
            # .trigger(processingTime="60 seconds") # Longer trigger = larger, more efficient Parquet files
            .start()
        )

        logger.info("Raw Event Archiver started. Writing to: %s", config.lake_root)
        logger.info(
            "Checkpoint location: %s",
            f"{config.lake_root}/_checkpoints/raw-events-archiver",
        )
        query.awaitTermination()
    except Exception as e:
        logger.error("Archiver job failed: %s", str(e))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
