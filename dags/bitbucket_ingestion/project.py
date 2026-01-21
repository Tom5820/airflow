from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import fetch_api_to_minio
from plugins.utils.common.get_config import CONFIG
from plugins.utils.common.spark_client import create_spark_job



default_args = {
    "owner": "lannh",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

execution_date = '{{ ds_nodash }}'

with DAG(
    dag_id="bitbucket_project",
    description="Fetch Bitbucket projects API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bitbucket", "minio", "api"],
) as dag:

    fetch_projects = PythonOperator(
        task_id="fetch_projects",
        python_callable=fetch_api_to_minio,
        op_kwargs={
            "api_url": f"https://api.bitbucket.org/2.0/workspaces/{CONFIG['bitbucket_workspace']}/projects",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/projects/date={execution_date}",
        },
    )

    fetch_projects
