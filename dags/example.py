from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.utils.spark_client import create_spark_job

default_args = {
    "owner": "lannh",
    "depends_on_past": False,
}


# def hello_world(**context):
#     print("Hello from Airflow!")
#     return "done"

with DAG(
    dag_id="spark_test_with_helper",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    spark_test = create_spark_job(
        task_id="spark_test_task",
        app_name="spark-test",
        # Upload test_script.py lên MinIO trước
        main_application_file="s3a://aiqg-spark-source/test.py",
        arguments=["--name", "{{ ds }}"],
        driver_memory="512m",
        executor_memory="512m",
        executor_instances=1
    )
    spark_test