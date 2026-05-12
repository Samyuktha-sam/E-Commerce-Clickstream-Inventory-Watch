import logging
import os
import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Setup Logger
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

    # Spark Connect URL
    spark_connect_url: str = os.getenv("SPARK_CONNECT_URL", "sc://localhost:15002")

    # Using StringType for timestamp initially to ensure stable parsing in Spark Connect
    CLICKSTREAM_SCHEMA: StructType = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )


def main() -> None:
    config = AppConfig()

    spark = (
        SparkSession.builder.appName("StorageLayer-RawEventArchiver")
        .remote(config.spark_connect_url)
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    query = None

    try:
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        archival_df = (
            raw_stream.select(
                F.from_json(
                    F.col("value").cast("string"), config.CLICKSTREAM_SCHEMA
                ).alias("data")
            )
            .select("data.*")
            .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
            .withColumn("event_date", F.to_date("event_time"))
            .filter(F.col("event_time").isNotNull())
        )

        logger.info("Raw Event Archiver starting. Writing to: %s", config.lake_root)

        query = (
            archival_df.writeStream.format("parquet")
            .queryName("RawEventArchiver")
            .option("path", "%s/raw-events" % config.lake_root)
            .option(
                "checkpointLocation",
                "%s/_checkpoints/raw-events-archiver" % config.lake_root,
            )
            .outputMode("append")
            .partitionBy("event_date")
            .trigger(processingTime="120 seconds")
            .start()
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Keyboard Interrupt detected. Initiating graceful shutdown...")
    except Exception as e:
        logger.error("Archiver job failed: %s", e)
    finally:
        if query and query.isActive:
            logger.info("Stopping streaming query: %s", query.id)
            query.stop()

        logger.info("Stopping Spark Session...")
        spark.stop()

        logger.info("Storage Layer shutdown complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()
