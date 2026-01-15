from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import fetch_api_to_minio
from plugins.utils.get_config import CONFIG


default_args = {
    "owner": "lannh",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

execution_date = '{{ ds_nodash }}'

with DAG(
    dag_id="bitbucket_repo",
    description="Fetch Bitbucket projects API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bitbucket", "minio", "api"],
) as dag:

    fetch_repos = PythonOperator(
        task_id="fetch_repos",
        python_callable=fetch_api_to_minio,
        op_kwargs={
            "api_url": "https://api.bitbucket.org/2.0/repositories/gemcorp",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/repository/date={execution_date}",
        },
    )

    fetch_repos
