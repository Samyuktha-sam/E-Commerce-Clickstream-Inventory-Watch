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
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    alerts_topic: str = os.getenv("ALERTS_TOPIC", "alerts-notifications")
    spark_connect_url: str = os.getenv("SPARK_CONNECT_URL", "sc://localhost:15002")
    spark_checkpoint: str = os.getenv(
        "SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/flash-sale"
    )

    # 1. Use StringType for the initial JSON extraction to prevent NullPointer errors
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
        SparkSession.builder.appName("SpeedLayer-FlashSaleDetector")
        .remote(config.spark_connect_url)
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

        decoded_df = (
            raw_stream.select(
                F.from_json(
                    F.col("value").cast("string"), config.CLICKSTREAM_SCHEMA
                ).alias("data")
            )
            .select("data.*")
            .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
            .filter(
                F.col("product_id").isNotNull()
                & F.col("event_time").isNotNull()
                & F.col("event_type").isNotNull()
            )
            .withWatermark("event_time", "10 minutes")
        )

        alerts_df = (
            decoded_df.groupBy(
                F.window("event_time", "5 minutes", "1 minute"), "product_id"
            )
            .agg(
                F.count(F.when(F.col("event_type") == "purchase", 1)).alias(
                    "purchase_count"
                ),
                F.count(F.when(F.col("event_type") == "view", 1)).alias("view_count"),
            )
            .filter((F.col("view_count") >= 5) & (F.col("purchase_count") < 3))
            .select(
                "window.start",
                "window.end",
                "product_id",
                "view_count",
                "purchase_count",
                F.lit("Flash Sale Recommended").alias("action"),
            )
        )

        # 3. Start the Sink using Lazy Formatting in Logs
        logger.info("Starting stream write to: %s", config.alerts_topic)

        query = (
            alerts_df.selectExpr("to_json(struct(*)) AS value")
            .writeStream.queryName("FlashSaleDetector")
            .outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("topic", config.alerts_topic)
            .option("checkpointLocation", f"{config.spark_checkpoint}/checkpoints")
            .option("failOnDataLoss", "false")
            .trigger(processingTime="30 seconds")
            .start()
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Keyboard Interrupt detected. Initiating graceful shutdown...")
    except Exception as e:
        logger.error("Unexpected Error during stream execution: %s", e)
    finally:
        if query and query.isActive:
            logger.info("Stopping streaming query: %s", query.id)
            query.stop()

        logger.info("Stopping Spark Session at: %s", config.spark_connect_url)
        spark.stop()

        logger.info("Shutdown complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()
