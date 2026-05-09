from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

S3_BUCKET = "s3a://ecommerce-clickstream-data"

spark = SparkSession.builder \
    .appName("DailyClickstreamBatchProcessingFromS3") \
    .getOrCreate()

input_path = f"{S3_BUCKET}/clickstream/raw/events/"
report_path = f"{S3_BUCKET}/clickstream/reports/daily_batch_report/"

df = spark.read.json(input_path)

# ==================================================
# 1. TOP 5 MOST VIEWED PRODUCTS
# ==================================================

top_products = df.filter(col("event_type") == "view") \
    .groupBy("product_id") \
    .agg(count("*").alias("view_count")) \
    .orderBy(col("view_count").desc()) \
    .limit(5)

print("\n===== TOP 5 MOST VIEWED PRODUCTS =====")
top_products.show(truncate=False)

# ==================================================
# 2. DAILY USER SEGMENTATION
# ==================================================

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

print("\n===== DAILY USER SEGMENTATION =====")
user_segments.show(truncate=False)

# ==================================================
# SAVE REPORTS BACK TO S3
# ==================================================

top_products.write.mode("overwrite").json(
    f"{report_path}/top_products"
)

user_segments.write.mode("overwrite").json(
    f"{report_path}/user_segments"
)

print("\nDaily Spark batch report generated successfully from S3.")

spark.stop()