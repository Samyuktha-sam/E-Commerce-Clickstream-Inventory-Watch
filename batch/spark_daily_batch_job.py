from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("DailyClickstreamBatchProcessing") \
    .getOrCreate()

input_path = "storage/raw/events.jsonl"
report_path = "storage/reports/daily_batch_report"

df = spark.read.json(input_path)

top_products = df.filter(col("event_type") == "view") \
    .groupBy("product_id") \
    .agg(count("*").alias("view_count")) \
    .orderBy(col("view_count").desc()) \
    .limit(5)

user_summary = df.groupBy("user_id").agg(
    count(when(col("event_type") == "view", True)).alias("views"),
    count(when(col("event_type") == "purchase", True)).alias("purchases")
)

user_segments = user_summary.withColumn(
    "segment",
    when(col("purchases") > 0, "Buyer")
    .when((col("views") >= 5) & (col("purchases") == 0), "Window Shopper")
    .otherwise("Casual Visitor")
)

top_products.write.mode("overwrite").json(f"{report_path}/top_products")
user_segments.write.mode("overwrite").json(f"{report_path}/user_segments")

print("Daily Spark batch processing completed successfully.")

spark.stop()