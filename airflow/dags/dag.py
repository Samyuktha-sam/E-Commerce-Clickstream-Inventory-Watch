from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.models import Connection
from minio import Minio

# Default settings for the DAG
default_args = {
    "owner": "data_eng",
    "retries": 0,
    # "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="minio_native_metadata_checker",
    default_args=default_args,
    start_date=datetime(2026, 5, 11),
    schedule="@daily", # <--- CHANGED THIS LINE
    catchup=False,
    tags=["minio_sdk", "raw_events"],
)
def minio_sdk_dag():

    @task
    def get_partition_metadata(ds=None):
        """
        ds: The logical date string (e.g. '2026-05-11')
        """
        # 1. Setup Client (Hardcoded here, but see tip below for Airflow Connections)
        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

        bucket_name = "clickstream-lake"
        prefix = f"raw-events/event_date={ds}/"

        # 2. List and Aggregate
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)

        file_count = 0
        total_size = 0
        latest_update = None

        # Iterate through the generator
        for obj in objects:
            file_count += 1
            total_size += obj.size
            if latest_update is None or obj.last_modified > latest_update:
                latest_update = obj.last_modified

        if file_count == 0:
            print(f"⚠️ No data found for prefix: {prefix}")
            return {"exists": False, "partition": prefix}

        # 3. Compile Results
        summary = {
            "exists": True,
            "partition": prefix,
            "file_count": file_count,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "last_modified": latest_update.isoformat(),
        }

        print(f"✅ Partition Metadata: {summary}")
        return summary

    # Execute task
    get_partition_metadata()


# Instantiate the DAG
minio_sdk_dag_instance = minio_sdk_dag()