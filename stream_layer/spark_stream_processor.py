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

    spark = (
        SparkSession.builder.appName("ClickstreamFlashSaleDetector")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
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
        .load()
    )

    decoded_df = (
        raw_stream.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp").cast("timestamp")))
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
    )

    windowed_stats = decoded_df.groupBy(
        F.window(F.col("event_time"), "5 minutes", "1 minute"), F.col("product_id")
    ).agg(
        F.count(F.when(F.col("event_type") == "purchase", True)).alias(
            "purchase_count"
        ),
        F.count(F.when(F.col("event_type") == "add_to_cart", True)).alias(
            "add_to_cart_count"
        ),
        F.count(F.when(F.col("event_type") == "view", True)).alias("view_count"),
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

    query = (
        alerts_df.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    print("Streaming started... Waiting for data from Kafka.")

    query.awaitTermination()


if __name__ == "__main__":
    main()
