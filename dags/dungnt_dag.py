from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from task_2 (PythonOperator)!")


with DAG(
    dag_id="simple_3_tasks_dag",
    description="A simple DAG with 3 basic tasks",
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "simple"],
) as dag:

    task_1 = BashOperator(
        task_id="task_1_print_date",
        bash_command="date",
    )

    task_2 = PythonOperator(
        task_id="task_2_say_hello",
        python_callable=say_hello,
    )

    task_3 = BashOperator(
        task_id="task_3_list_files",
        bash_command="ls -la",
    )

    task_1 >> task_2 >> task_3
