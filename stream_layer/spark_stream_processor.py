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
    # Separate checkpoint path to avoid interference
    spark_checkpoint: str = os.getenv(
        "SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/flash-sale-detector"
    )

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
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )
        # 1. State Store Optimization (CRITICAL for Flash Sales)
        # Replaces default In-Memory store with RocksDB.
        # Better for large stateful windows and prevents OOM during spikes.
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        # 2. Shuffle & Task Tuning
        # Since we are using 2 cores per executor, 4 partitions is a good "sweet spot."
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        # 3. Suppress Adaptive Query Execution (AQE) Warnings
        # As seen in your previous logs, AQE isn't supported in Streaming. Let's turn it off.
        .config("spark.sql.adaptive.enabled", "false")
        # 4. Metadata & Checkpoint Cleanup
        # Prevents the checkpoint directory from growing indefinitely by limiting history.
        .config("spark.sql.streaming.minBatchesToRetain", "20")
        # 5. Reliability & Performance
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


def detect_flash_sales(df: DataFrame) -> DataFrame:
    """Identifies products with high interest (views) but low conversion (purchases)."""
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
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        decoded_df = transform_raw_stream(raw_stream, config.CLICKSTREAM_SCHEMA)
        alerts_df = detect_flash_sales(decoded_df)

        # Sink: Publish alerts back to Kafka for downstream microservices
        query = (
            alerts_df.selectExpr("to_json(struct(*)) AS value")
            .writeStream.outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("topic", config.alerts_topic)
            .option("checkpointLocation", f"{config.spark_checkpoint}/checkpoints")
            .trigger(processingTime="30 seconds")
            .start()
        )

        logger.info(
            "Flash Sale Detector started. Listening for high-view/low-purchase patterns."
        )
        query.awaitTermination()
    except (Py4JJavaError, StreamingQueryException) as e:
        logger.error("Flash Sale Detector failed: %s", str(e))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
