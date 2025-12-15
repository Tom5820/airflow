import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from plugins.utils.airflow_callbacks import dag_failure_callback


def success_task():
    print("Task này chạy OK")


def fail_task():
    raise Exception("Cố tình fail để test DAG failure callback")


with DAG(
    dag_id="example_dag_task_fail",
    start_date=pendulum.datetime(2025, 12, 15, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=dag_failure_callback,
    tags=["example", "failure-test"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_ok = PythonOperator(
        task_id="task_ok",
        python_callable=success_task,
    )

    task_fail = PythonOperator(
        task_id="task_fail",
        python_callable=fail_task,
    )

    end = EmptyOperator(task_id="end")

    start >> task_ok >> task_fail >> end
