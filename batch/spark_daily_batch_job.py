from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from batch.report_utils import build_summary_text
from schemas.product_catalog import PRODUCT_CATEGORY_BY_ID


PRODUCT_CATEGORY_MAP = F.create_map(
    *[value for item in PRODUCT_CATEGORY_BY_ID.items() for value in (F.lit(item[0]), F.lit(item[1]))]
)


def create_spark_session() -> SparkSession:
    lake_root = os.getenv("MINIO_LAKE_PATH", "s3a://clickstream-lake")
    spark = (
        SparkSession.builder.appName("DailyClickstreamBatchProcessing")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.jars.packages", os.getenv("SPARK_EXTRA_PACKAGES", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("pipeline.report.root", f"{lake_root}/reports")
    return spark


def with_product_category(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "product_category",
        F.coalesce(PRODUCT_CATEGORY_MAP[F.col("product_id")], F.lit("Other")),
    )


def collect_rows(df: DataFrame) -> list[dict[str, object]]:
    return [row.asDict(recursive=True) for row in df.collect()]


def publish_batch_results(payload: dict[str, object]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        producer.send(os.getenv("BATCH_RESULTS_TOPIC", "batch-results"), payload).get(timeout=30)
    finally:
        producer.flush()
        producer.close()


def main() -> None:
    spark = create_spark_session()
    lake_root = os.getenv("MINIO_LAKE_PATH", "s3a://clickstream-lake")
    report_date = datetime.now(timezone.utc).date().isoformat()
    report_root = f"{lake_root}/reports/report_date={report_date}"
    local_report_dir = Path(os.getenv("LOCAL_REPORT_DIR", "/opt/airflow/reports"))
    local_report_dir.mkdir(parents=True, exist_ok=True)
    local_report_file = local_report_dir / "daily_summary.txt"

    try:
        raw_events = spark.read.format("delta").load(f"{lake_root}/raw-events")
        daily_events = with_product_category(raw_events).filter(F.col("event_date") == F.lit(report_date))

        if daily_events.limit(1).count() == 0:
            raise RuntimeError("No events available for the current reporting day.")

        top_products_df = (
            daily_events.filter(F.col("event_type") == "view")
            .groupBy("product_id")
            .agg(F.count("*").alias("view_count"))
            .orderBy(F.col("view_count").desc(), F.col("product_id"))
            .limit(5)
        )

        user_segments_df = (
            daily_events.groupBy("user_id")
            .agg(
                F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
                F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
            )
            .withColumn(
                "segment",
                F.when(F.col("purchases") > 0, F.lit("Buyer"))
                .when((F.col("views") >= 5) & (F.col("purchases") == 0), F.lit("Window Shopper"))
                .otherwise(F.lit("Casual Visitor")),
            )
        )

        segment_counts_df = (
            user_segments_df.groupBy("segment")
            .agg(F.count("*").alias("user_count"))
            .orderBy(F.col("user_count").desc(), F.col("segment"))
        )

        conversion_rates_df = (
            daily_events.groupBy("product_category")
            .agg(
                F.count(F.when(F.col("event_type") == "view", 1)).alias("view_count"),
                F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
            )
            .withColumn(
                "conversion_rate",
                F.when(F.col("view_count") > 0, F.col("purchase_count") / F.col("view_count")).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("conversion_rate").desc(), F.col("product_category"))
            .select(
                F.col("product_category").alias("category"),
                "view_count",
                "purchase_count",
                "conversion_rate",
            )
        )

        top_products_df.write.mode("overwrite").json(f"{report_root}/top_products")
        segment_counts_df.write.mode("overwrite").json(f"{report_root}/user_segments")
        conversion_rates_df.write.mode("overwrite").json(f"{report_root}/conversion_rates")

        top_products = collect_rows(top_products_df)
        segment_counts = collect_rows(segment_counts_df)
        conversion_rates = collect_rows(conversion_rates_df)

        summary_text = build_summary_text(report_date, top_products, segment_counts, conversion_rates)
        local_report_file.write_text(summary_text, encoding="utf-8")

        publish_batch_results(
            {
                "report_date": report_date,
                "top_products": top_products,
                "segment_counts": segment_counts,
                "conversion_rates": conversion_rates,
                "report_path": report_root,
            }
        )
        print(f"Daily batch report completed successfully for {report_date}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
