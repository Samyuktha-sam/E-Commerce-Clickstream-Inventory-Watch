import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    from_json,
    lit,
    sum as _sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import StringType, StructField, StructType


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "clickstream-events")

    spark = (
        SparkSession.builder.appName("ClickstreamFlashSaleDetector")
        .master("spark://spark-master:7077")
        # Ensure the Scala version (2.13) and Package version match your environment
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )
        # Optimization: Shuffle partitions default to 200, which is overkill for a small Docker setup
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

    events = (
        raw_stream.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .filter(col("event_time").isNotNull())
    )

    aggregated = (
        events.groupBy(
            window(col("event_time"), "10 minutes", "1 minute"), col("product_id")
        )
        .agg(
            _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
            _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
        )
        .filter((col("views") > 100) & (col("purchases") < 5))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("views"),
            col("purchases"),
            concat(
                lit("High Interest, Low Conversion for product "),
                col("product_id"),
                lit(" -> suggest Flash Sale / discount"),
            ).alias("notification"),
        )
    )

    query = (
        aggregated.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "50")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
