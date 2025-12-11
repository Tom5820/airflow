from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# from plugins.utils.spark_client import create_spark_job

default_args = {
    "owner": "lannh",
    "depends_on_past": False,
}


def hello_world(**context):
    print("Hello from Airflow!")
    return "done"

with DAG(
    dag_id="example_simple_dag",
    default_args=default_args,
    description="A simple example DAG",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    task_hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world
    )

    task_hello

    # spark_test = create_spark_job(
    #     task_id="spark_test_task",
    #     app_name="spark-test",
    #     # Upload test_script.py lên MinIO trước
    #     py_file="s3a://aiqg-spark-source/test.py",
    #     arguments=["--name", "{{ ds }}"],
    #     driver_memory="512m",
    #     executor_memory="512m",
    #     executor_instances=1
    # )