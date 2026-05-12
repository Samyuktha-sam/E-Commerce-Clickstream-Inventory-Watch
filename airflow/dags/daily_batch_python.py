"""
Pure Python version of daily batch processing without PySpark.
Reads from MinIO S3, processes data with pandas, writes results back.
Can be used with Airflow's PythonOperator.
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# MinIO S3 Configuration
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-1:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("MINIO_BUCKET", "clickstream-lake")
INPUT_PREFIX_TEMPLATE = os.getenv("MINIO_INPUT_PREFIX", "raw-events/event_date={ds}")
OUTPUT_PREFIX_TEMPLATE = os.getenv(
    "MINIO_OUTPUT_PREFIX", "reports/daily_batch_report/event_date={ds}"
)


def get_s3_client():
    """Create and return boto3 S3 client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        use_ssl=False,
    )


def read_parquet_objects_from_s3(bucket: str, prefix: str, client=None):
    """Read all Parquet objects from S3 prefix and return a DataFrame."""
    if client is None:
        client = get_s3_client()

    dataframes = []

    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith(".parquet"):
                    logger.info(f"Reading: {key}")
                    response = client.get_object(Bucket=bucket, Key=key)
                    dataframes.append(pd.read_parquet(BytesIO(response["Body"].read())))

    except Exception as e:
        logger.error(f"Error reading from S3: {e}")
        raise

    if not dataframes:
        logger.info("Total records read: 0")
        return pd.DataFrame()

    df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Total records read: {len(df)}")
    return df


def write_dataframe_to_s3(
    df: pd.DataFrame, bucket: str, prefix: str, filename: str, client=None
):
    """Write pandas DataFrame to S3 as JSON lines."""
    if client is None:
        client = get_s3_client()

    try:
        json_buffer = BytesIO()
        for _, row in df.iterrows():
            json_buffer.write((json.dumps(row.to_dict()) + "\n").encode("utf-8"))

        json_buffer.seek(0)
        s3_key = f"{prefix}/{filename}"

        client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json_buffer.getvalue(),
            ContentType="application/json",
        )

        logger.info(f"✅ Written to S3: s3://{bucket}/{s3_key} ({len(df)} records)")

    except Exception as e:
        logger.error(f"Error writing to S3: {e}")
        raise


def daily_batch_processing(**context):
    """
    Main batch processing function.
    Designed to be called from Airflow PythonOperator.
    """
    logger.info("=" * 80)
    logger.info("🚀 Starting Daily Batch Processing (Pure Python)")
    logger.info("=" * 80)

    try:
        date_str = context.get("ds") or datetime.now().strftime("%Y-%m-%d")
        input_prefix = INPUT_PREFIX_TEMPLATE.format(ds=date_str)
        output_prefix = OUTPUT_PREFIX_TEMPLATE.format(ds=date_str)

        s3_client = get_s3_client()
        logger.info(f"✅ Connected to MinIO S3: {S3_ENDPOINT}")

        logger.info(f"📥 Reading events from: s3://{S3_BUCKET}/{input_prefix}")
        df = read_parquet_objects_from_s3(S3_BUCKET, input_prefix, s3_client)

        if df.empty:
            logger.warning("⚠️ No records found in input path")
            return {"status": "warning", "message": "No records found"}

        logger.info(f"📊 DataFrame shape: {df.shape}")
        logger.info(f"📋 Columns: {list(df.columns)}")

        # Analysis 1: Top 5 Products
        logger.info("\n" + "=" * 80)
        logger.info("Analysis 1: Top 5 Products by View Count")
        logger.info("=" * 80)

        view_events = df[df["event_type"] == "view"].copy()
        top_products = (
            view_events.groupby("product_id")
            .size()
            .reset_index(name="view_count")
            .sort_values("view_count", ascending=False)
            .head(5)
        )

        logger.info(f"\n✅ Top Products:\n{top_products.to_string()}")
        write_dataframe_to_s3(
            top_products, S3_BUCKET, output_prefix, "top_products.json", s3_client
        )

        # Analysis 2: User Summary
        logger.info("\n" + "=" * 80)
        logger.info("Analysis 2: User Summary (Views & Purchases)")
        logger.info("=" * 80)

        user_summary = (
            df.groupby("user_id")
            .agg(
                {
                    "event_type": [
                        lambda x: (x == "view").sum(),
                        lambda x: (x == "purchase").sum(),
                    ]
                }
            )
            .reset_index()
        )

        user_summary.columns = ["user_id", "views", "purchases"]
        logger.info(
            f"\n✅ User Summary (first 10 rows):\n{user_summary.head(10).to_string()}"
        )

        # Analysis 3: User Segmentation
        logger.info("\n" + "=" * 80)
        logger.info("Analysis 3: User Segmentation")
        logger.info("=" * 80)

        def segment_user(row):
            if row["purchases"] > 0:
                return "Buyer"
            elif row["views"] >= 5 and row["purchases"] == 0:
                return "Window Shopper"
            else:
                return "Casual Visitor"

        user_segments = user_summary.copy()
        user_segments["segment"] = user_segments.apply(segment_user, axis=1)

        segment_counts = user_segments["segment"].value_counts()
        logger.info(f"\n✅ Segment Distribution:\n{segment_counts}")

        write_dataframe_to_s3(
            user_segments, S3_BUCKET, output_prefix, "user_segments.json", s3_client
        )

        logger.info("\n" + "=" * 80)
        logger.info("✅ BATCH PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        return {
            "status": "success",
            "records_processed": len(df),
            "unique_products": int(df["product_id"].nunique()),
            "unique_users": len(user_summary),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"❌ Batch processing failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    result = daily_batch_processing()
    print(f"\n🎯 Result: {result}")
