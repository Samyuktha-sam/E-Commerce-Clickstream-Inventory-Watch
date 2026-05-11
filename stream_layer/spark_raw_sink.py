import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType

from schemas.product_catalog import PRODUCT_CATEGORY_BY_ID

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PRODUCT_CATEGORY_MAP = F.create_map(
    *[value for item in PRODUCT_CATEGORY_BY_ID.items() for value in (F.lit(item[0]), F.lit(item[1]))]
)


@dataclass(frozen=True)
class AppConfig:
    """Configuration specifically for the data lake storage layer."""

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    lake_root: str = os.getenv("MINIO_LAKE_PATH", "s3a://clickstream-lake")
    spark_master: str = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    spark_packages: str = os.getenv(
        "SPARK_EXTRA_PACKAGES",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.2",
    )
    spark_checkpoint_base: str = os.getenv("SPARK_CHECKPOINT_BASE", "/opt/spark/checkpoints")
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

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
        .config("spark.jars.packages", config.spark_packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def transform_for_lake(raw_df: DataFrame, schema: StructType) -> DataFrame:
    return (
        raw_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "product_category",
            F.coalesce(PRODUCT_CATEGORY_MAP[F.col("product_id")], F.lit("Other")),
        )
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
            .option("startingOffsets", config.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        archival_df = transform_for_lake(raw_stream, config.CLICKSTREAM_SCHEMA)

        query = (
            archival_df.writeStream.format("delta")
            .option("path", f"{config.lake_root}/raw-events")
            .option(
                "checkpointLocation",
                f"{config.spark_checkpoint_base}/raw-events-archiver",
            )
            .outputMode("append")
            .partitionBy("event_date")
            .start()
        )

        logger.info("Raw Event Archiver started. Writing Delta data to %s/raw-events", config.lake_root)
        query.awaitTermination()
    except Exception as exc:
        logger.error("Archiver job failed: %s", str(exc))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
