# from pyspark.sql import DataFrame, SparkSession
# import pyspark.sql.functions as F
# from pyspark.sql.types import StringType, StructField, StructType

# # 1. Initialize Spark Session with Kafka dependencies
# spark: SparkSession = (
#     SparkSession.builder.appName("KafkaTestProducer")
#     .master("spark://spark-master:7077")
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
#     .config("spark.sql.shuffle.partitions", "4")  # Optimized for stateful windowing
#     .config("spark.streaming.stopGracefullyOnShutdown", "true")
#     .getOrCreate()
# )

# schema: StructType = StructType(
#         [
#             StructField("event_id", StringType(), True),
#             StructField("user_id", StringType(), True),
#             StructField("product_id", StringType(), True),
#             StructField("event_type", StringType(), True),
#             StructField("timestamp", StringType(), True),
#         ]
#     )

# # 2. Create sample data
# raw_stream = (
#     spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers","broker:29092")
#     .option("subscribe", 'clickstream-events')
#     .option("startingOffsets", "latest")
#     .load()
# )
# # 3. Transform to Kafka format (must have a 'value' column)
# # We convert the entire row into a JSON string
# kafka_df = (
#         raw_stream.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
#         .select("data.*")
#         .withColumn("event_time", F.col("timestamp").cast("timestamp"))
#         .filter(F.col("event_time").isNotNull())
#         .withWatermark("event_time", "10 minutes")
#     )

# # 4. Example transformations
# transformed_df: DataFrame = (
#     kafka_df
#     .withColumn("event_type", F.lower(F.col("event_type")))
#     .withColumn("event_date", F.to_date("event_time"))
#     .withColumn("user_product_key", F.concat_ws("-", "user_id", "product_id"))
#     .withColumn("is_purchase", F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0)))
# )


# # 5. Console log (streaming)
# console_query = (
#     transformed_df
#     .writeStream
#     .format("console")
#     .option("truncate", "false")
#     .option("numRows", 50)
#     .outputMode("update")
#     .trigger(processingTime="30 seconds")
#     .start()
# )

# # Keep the stream running and print rows to console
# console_query.awaitTermination()
# spark.stop()

# --------------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession
# import os


# def create_spark_session():
#     # We define the packages here, but it's better to pass them via spark-submit
#     return (
#         SparkSession.builder.appName("S3IngestionTest")
#         .config(
#             "spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
#         )
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .getOrCreate()
#     )


# def main():
#     spark = create_spark_session()

#     # # --- S3A CONFIGURATION ---
#     # # In a real prod environment, use Instance Profiles or Secret Manager.
#     # # For testing, you can set these as environment variables or hardcode them here.
#     # hadoop_conf = spark._jsc.hadoopConfiguration()
#     # hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY", "your_access_key"))
#     # hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY", "your_secret_key"))

#     # # If using MinIO, uncomment the lines below:
#     # # hadoop_conf.set("fs.s3a.endpoint", "http://broker:9000") # Use your endpoint
#     # # hadoop_conf.set("fs.s3a.path.style.access", "true")
#     # # hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

#     s3_path = "s3a://clickstream-lake/raw-events/event_date=2026-05-10"

#     try:
#         print(f"--- Attempting to read from: {s3_path} ---")

#         # 1. Read the data
#         df = spark.read.parquet(s3_path)

#         # 2. Basic Validation
#         print("--- Schema Discovery ---")
#         df.printSchema()

#         row_count = df.count()
#         print(f"--- Data Ingested Successfully: {row_count} rows found ---")

#         print("--- Sample Data ---")
#         df.show(5, truncate=False)

#     except Exception as e:
#         print(f"!!! INGESTION ERROR !!!\n{e}")
#     finally:
#         spark.stop()


# if __name__ == "__main__":
#     main()

# ------------------------------------------------------------------------------------------
#


# from pyspark.sql import SparkSession
# import os
# import traceback
# from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# CLICKSTREAM_LAKE_SCHEMA = StructType(
#     [
#         StructField("event_id", StringType(), True),
#         StructField("user_id", StringType(), True),
#         StructField("product_id", StringType(), True),
#         StructField("event_type", StringType(), True),
#         StructField("timestamp", StringType(), True),
#         StructField("event_time", TimestampType(), True),
#     ]
# )


# def get_sample_parquet_file(spark: SparkSession, directory_path: str) -> str:
#     hadoop_conf = spark._jsc.hadoopConfiguration()
#     path = spark._jvm.org.apache.hadoop.fs.Path(directory_path)
#     file_system = path.getFileSystem(hadoop_conf)

#     parquet_files = [
#         status.getPath().toString()
#         for status in file_system.listStatus(path)
#         if status.isFile() and status.getPath().getName().endswith(".parquet")
#     ]

#     if not parquet_files:
#         raise RuntimeError(f"No parquet files found under {directory_path}")

#     print(f"--- Partition file count: {len(parquet_files)} parquet files ---")
#     return parquet_files[0]


# def create_spark_session():
#     # We define the packages here, but it's better to pass them via spark-submit
#     return (
#         SparkSession.builder.appName("S3IngestionTest")
#         .config(
#             "spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2",
#         )
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .getOrCreate()
#     )


# def main():
#     spark = create_spark_session()

#     # # --- S3A CONFIGURATION ---
#     # # In a real prod environment, use Instance Profiles or Secret Manager.
#     # # For testing, you can set these as environment variables or hardcode them here.
#     # hadoop_conf = spark._jsc.hadoopConfiguration()
#     # hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY", "your_access_key"))
#     # hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY", "your_secret_key"))

#     # # If using MinIO, uncomment the lines below:
#     # # hadoop_conf.set("fs.s3a.endpoint", "http://broker:9000") # Use your endpoint
#     # # hadoop_conf.set("fs.s3a.path.style.access", "true")
#     # # hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

#     s3_path = "s3a://clickstream-lake/raw-events/event_date=2026-05-10"

#     try:
#         print(f"--- Attempting to read from: {s3_path} ---")
#         sample_file_path = get_sample_parquet_file(spark, s3_path)
#         print(f"--- Sampling file: {sample_file_path} ---")

#         # 1. Read a representative parquet object from the partition
#         df = spark.read.schema(CLICKSTREAM_LAKE_SCHEMA).parquet(sample_file_path)

#         # 2. Basic Validation
#         print("--- Schema Discovery ---")
#         df.printSchema()

#     except Exception as e:
#         print(f"!!! INGESTION ERROR !!!\n{e}")
#         traceback.print_exc()
#     finally:
#         spark.stop()


# if __name__ == "__main__":
#     main()
