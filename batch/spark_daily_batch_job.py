from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, current_date

S3_BUCKET = "s3a://ecommerce-clickstream-data"
INPUT_PATH = f"{S3_BUCKET}/clickstream/raw/events/"
REPORT_PATH = f"{S3_BUCKET}/clickstream/reports/daily_batch_report/"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DailyClickstreamBatchProcessingFromS3")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    df = spark.read.json(INPUT_PATH)

    top_products = (
        df.filter(col("event_type") == "view")
        .groupBy("product_id")
        .agg(count("*").alias("view_count"))
        .orderBy(col("view_count").desc())
        .limit(5)
        .withColumn("report_date", current_date())
    )

    user_summary = df.groupBy("user_id").agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
    )

    user_segments = (
        user_summary.withColumn(
            "segment",
            when(col("purchases") > 0, "Buyer")
            .when((col("views") >= 5) & (col("purchases") == 0), "Window Shopper")
            .otherwise("Casual Visitor"),
        )
        .withColumn("report_date", current_date())
    )

    top_products.write.mode("overwrite").json(f"{REPORT_PATH}/top_products")
    user_segments.write.mode("overwrite").json(f"{REPORT_PATH}/user_segments")

    print("Daily Spark batch processing from S3 completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()