from datetime import datetime, timedelta
from airflow.sdk import dag, task  # Use decorators

default_args = {
    "owner": "airflow",
    "retries": 0,
}


@dag(
    dag_id="hello_world_dag",
    default_args=default_args,
    description="A simple hello world DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "airflow_3"],
)
def hello_world_workflow():  # Renamed function to avoid shadowing the 'dag' decorator

    @task
    def hello_world():
        print("Hello World! Airflow 3 is working properly.")

    # In TaskFlow, calling the function creates the task
    hello_world()


# Call the function to register the DAG
hello_world_workflow()
