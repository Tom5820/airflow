from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket_export import fetch_api_to_minio

default_args = {
    "owner": "lannh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bitbucket_member",
    description="Fetch Bitbucket members API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bitbucket", "minio", "api"],
) as dag:

    fetch_members = PythonOperator(
        task_id="fetch_members",
        python_callable=fetch_api_to_minio,
        op_kwargs={
            "api_url": "https://api.bitbucket.org/2.0/workspaces/gemcorp/members",
            "bucket_name": "raw",
            "aws_conn_id": "aws_minio",
            "object_prefix": "bitbucket/members/dt={{ ds }}",
        },
    )

    fetch_members
