"""
Serving Layer — Batch bridge: MinIO Parquet → ClickHouse
Triggered daily by Airflow after the Spark batch job completes.

Reads two Spark output directories from MinIO:
  s3a://clickstream-lake/reports/top_products/   → batch_top_products
  s3a://clickstream-lake/reports/user_segments/  → batch_user_segments

Uses boto3 (S3-compatible) to list/read Parquet files and
clickhouse-driver to bulk-insert into ClickHouse.
"""

import io
import logging
import os
from datetime import date, datetime

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from clickhouse_driver import Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "clickstream-lake")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB   = os.getenv("CLICKHOUSE_DB", "clickstream")

# Spark writes daily partitioned output under these prefixes
TOP_PRODUCTS_PREFIX  = os.getenv("TOP_PRODUCTS_PREFIX",  "reports/top_products/")
USER_SEGMENTS_PREFIX = os.getenv("USER_SEGMENTS_PREFIX", "reports/user_segments/")

REPORT_DATE = date.fromisoformat(
    os.getenv("REPORT_DATE", date.today().isoformat())
)


# ---------------------------------------------------------------------------
# S3 / MinIO helpers
# ---------------------------------------------------------------------------

def get_s3_client():
    """Return a boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def list_parquet_keys(s3, prefix: str) -> list[str]:
    """List all .parquet object keys under a given prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    return keys


def read_parquet_from_s3(s3, key: str):
    """Download a Parquet file from MinIO and return as a PyArrow table."""
    response = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pq.read_table(buffer)


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def get_ch_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
    )


def load_top_products(s3, ch: Client, report_date: date) -> int:
    """
    Read top-products Parquet from MinIO, add rank, insert into
    batch_top_products.  Returns number of rows inserted.
    """
    prefix = TOP_PRODUCTS_PREFIX
    keys   = list_parquet_keys(s3, prefix)

    if not keys:
        logger.warning("No Parquet files found under %s", prefix)
        return 0

    rows_inserted = 0
    for key in keys:
        table = read_parquet_from_s3(s3, key)
        df    = table.to_pydict()  # {col: [values]}

        product_ids  = df.get("product_id", [])
        view_counts  = df.get("view_count", [])

        # Sort descending by view_count (Spark already does this, but be safe)
        combined = sorted(
            zip(product_ids, view_counts),
            key=lambda x: x[1],
            reverse=True,
        )

        data = [
            {
                "report_date": report_date,
                "product_id":  pid,
                "view_count":  int(vc),
                "rank":        rank,
            }
            for rank, (pid, vc) in enumerate(combined, start=1)
        ]

        if data:
            ch.execute(
                "INSERT INTO batch_top_products "
                "(report_date, product_id, view_count, rank) VALUES",
                data,
            )
            rows_inserted += len(data)
            logger.info(
                "Inserted %d top-product rows for %s from key %s",
                len(data), report_date, key,
            )

    return rows_inserted


def load_user_segments(s3, ch: Client, report_date: date) -> int:
    """
    Read user-segments Parquet from MinIO, insert into batch_user_segments.
    Returns number of rows inserted.
    """
    prefix = USER_SEGMENTS_PREFIX
    keys   = list_parquet_keys(s3, prefix)

    if not keys:
        logger.warning("No Parquet files found under %s", prefix)
        return 0

    rows_inserted = 0
    for key in keys:
        table = read_parquet_from_s3(s3, key)
        df    = table.to_pydict()

        user_ids   = df.get("user_id",   [])
        views      = df.get("views",     [])
        purchases  = df.get("purchases", [])
        segments   = df.get("segment",   [])

        data = [
            {
                "report_date": report_date,
                "user_id":     str(uid),
                "views":       int(v),
                "purchases":   int(p),
                "segment":     str(seg),
            }
            for uid, v, p, seg in zip(user_ids, views, purchases, segments)
        ]

        if data:
            ch.execute(
                "INSERT INTO batch_user_segments "
                "(report_date, user_id, views, purchases, segment) VALUES",
                data,
            )
            rows_inserted += len(data)
            logger.info(
                "Inserted %d user-segment rows for %s from key %s",
                len(data), report_date, key,
            )

    return rows_inserted


# ---------------------------------------------------------------------------
# Entry point (also callable from Airflow PythonOperator)
# ---------------------------------------------------------------------------

def run(report_date: date = REPORT_DATE) -> dict:
    """
    Main load function.
    Returns a summary dict so Airflow can log it as XCom.
    """
    logger.info(
        "Starting batch→ClickHouse load for date=%s  minio=%s  ch=%s:%s/%s",
        report_date, MINIO_ENDPOINT, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB,
    )

    s3 = get_s3_client()
    ch = get_ch_client()

    top_rows  = load_top_products(s3, ch, report_date)
    seg_rows  = load_user_segments(s3, ch, report_date)

    summary = {
        "report_date":         str(report_date),
        "top_products_rows":   top_rows,
        "user_segments_rows":  seg_rows,
        "loaded_at":           datetime.utcnow().isoformat(),
    }
    logger.info("Load complete: %s", summary)
    return summary


if __name__ == "__main__":
    run()