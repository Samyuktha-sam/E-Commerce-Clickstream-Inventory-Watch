import logging
import os
from dataclasses import dataclass

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql.types import StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AppConfig:
    """Configuration specifically for the Speed Layer alerting system."""

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    alerts_topic: str = os.getenv("ALERTS_TOPIC", "alerts-notifications")
    spark_master: str = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    spark_packages: str = os.getenv(
        "SPARK_EXTRA_PACKAGES",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.2",
    )
    spark_checkpoint_base: str = os.getenv("SPARK_CHECKPOINT_BASE", "/opt/spark/checkpoints")
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
    flash_sale_view_threshold: int = int(os.getenv("FLASH_SALE_VIEW_THRESHOLD", "100"))
    flash_sale_purchase_threshold: int = int(os.getenv("FLASH_SALE_PURCHASE_THRESHOLD", "5"))
    flash_sale_window_duration: str = os.getenv("FLASH_SALE_WINDOW_DURATION", "10 minutes")
    flash_sale_slide_duration: str = os.getenv("FLASH_SALE_SLIDE_DURATION", "1 minute")

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
        SparkSession.builder.appName("SpeedLayer-FlashSaleDetector")
        .master(config.spark_master)
        .config("spark.jars.packages", config.spark_packages)
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.streaming.minBatchesToRetain", "20")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.sql.streaming.checkpointFileManager.class",
            "org.apache.spark.sql.execution.streaming.FileContextBasedCheckpointFileManager",
        )
        .getOrCreate()
    )


def transform_raw_stream(raw_df: DataFrame, schema: StructType) -> DataFrame:
    return (
        raw_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
    )


def detect_flash_sales(df: DataFrame, config: AppConfig) -> DataFrame:
    """Identifies products with high interest (views) but low conversion (purchases)."""
    windowed_stats = df.groupBy(
        F.window(
            "event_time",
            config.flash_sale_window_duration,
            config.flash_sale_slide_duration,
        ),
        "product_id",
    ).agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.count(F.when(F.col("event_type") == "view", 1)).alias("view_count"),
    )

    return windowed_stats.filter(
        (F.col("view_count") > config.flash_sale_view_threshold)
        & (F.col("purchase_count") < config.flash_sale_purchase_threshold)
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
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
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", config.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        decoded_df = transform_raw_stream(raw_stream, config.CLICKSTREAM_SCHEMA)
        alerts_df = detect_flash_sales(decoded_df, config)

        query = (
            alerts_df.selectExpr("to_json(struct(*)) AS value")
            .writeStream.outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("topic", config.alerts_topic)
            .option(
                "checkpointLocation",
                f"{config.spark_checkpoint_base}/flash-sale-detector",
            )
            .trigger(processingTime="30 seconds")
            .start()
        )

        logger.info(
            "Flash Sale Detector started with %s-minute window and thresholds views>%s purchases<%s.",
            config.flash_sale_window_duration,
            config.flash_sale_view_threshold,
            config.flash_sale_purchase_threshold,
        )
        query.awaitTermination()
    except (Py4JJavaError, StreamingQueryException) as exc:
        logger.error("Flash Sale Detector failed: %s", str(exc))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
