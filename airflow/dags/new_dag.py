from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.models import Connection
from minio import Minio

default_args = {
    "owner": "data_eng",
    "retries": 0,
    # "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="spark_submit_test",
    default_args=default_args,
    start_date=datetime(2026, 5, 11),
    schedule="@daily",
    catchup=False,
    tags=["spark", "raw_events"],
)
def spark_submit_dag():
    @task.pyspark(conn_id="spark_default", config_kwargs={"spark.remote": "sc://spark-master:15002"} )
    def verify_spark(spark=None):
        # If this code runs, the connection is successful
        print("Successfully connected to Spark!")

        # Simple data test
        try:
            print(f"Connected to Spark version {spark.conf}")
            # Test it
            df = spark.range(10)
            df.show(5)

            # Or read from mounted volume
            # df = spark.read.parquet("/opt/spark/work-dir/data/mydata")
        except Exception as e:
            print(f"Error: {e}")
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            try:
                spark.stop()
                print("Spark session stopped.")
            except Exception as e:
                print(f"Error stopping Spark: {e}")

        return "Connection Verified"

    verify_spark()

spark_submit_dag_instance = spark_submit_dag()
