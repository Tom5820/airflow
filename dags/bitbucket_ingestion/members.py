from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket_export import fetch_api_to_minio
from plugins.utils.get_config import CONFIG


default_args = {
    "owner": "lannh",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

execution_date = '{{ execution_date.strftime(\'%y%m%d\') }}'

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
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/members/dt={execution_date}",
        },
    )

    fetch_members
