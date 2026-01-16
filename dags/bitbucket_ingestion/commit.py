from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.decorators import task

from plugins.utils.bitbucket.bitbucket_export import list_repos, fetch_api_to_minio
from plugins.utils.common.get_config import CONFIG


default_args = {
    "owner": "lannh",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

execution_date = '{{ ds_nodash }}'

with DAG(
    dag_id="bitbucket_commit",
    description="Fetch Bitbucket commit API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bitbucket", "minio", "api"],
    max_active_tasks=10
) as dag:
    list_repos_task = PythonOperator(
        task_id="list_repos",
        python_callable=list_repos,
        op_kwargs={
            "workspace": CONFIG["bitbucket_workspace"],
        },
    )


    @task
    def build_op_kwargs(api_url: str) -> dict:
        """Build kwargs dict for each api_url"""
        execution_date = '{{ ds_nodash }}'
        return {
            "api_url": api_url,
            "bucket_name": CONFIG["raw_bucket"],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/commit/date={execution_date}",
        }


    # Expand để tạo kwargs cho từng URL
    op_kwargs_list = build_op_kwargs.expand(api_url=list_repos_task.output)

    # Expand PythonOperator với kwargs đã build
    fetch_commit = PythonOperator.partial(
        task_id="process_repo",
        python_callable=fetch_api_to_minio,
    ).expand(op_kwargs=op_kwargs_list)

    list_repos_task >> op_kwargs_list >> fetch_commit
