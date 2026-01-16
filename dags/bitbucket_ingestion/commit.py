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
    max_active_tasks=5
) as dag:
    list_repos_task = PythonOperator(
        task_id="list_repos",
        python_callable=list_repos,
        op_kwargs={
            "workspace": CONFIG["bitbucket_workspace"],
        },
    )

    fetch_commit = PythonOperator.partial(
        task_id="process_repo",
        python_callable=fetch_api_to_minio,
        op_kwargs={  # các giá trị chung, không đổi theo từng repo
            "bucket_name": CONFIG["raw_bucket"],
            "aws_conn_id": "minio_connection",
        },
    ).expand(
        op_kwargs=list_repos_task.output.map(
            lambda api_url: {
                "api_url": api_url,
                "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/commit/date={{ ds_nodash }}",
            }
        )
    )

    list_repos_task >> fetch_commit
