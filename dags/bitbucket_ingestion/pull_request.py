from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import fetch_entity_by_repo
from plugins.utils.common.get_config import CONFIG


default_args = {
    "owner": "lannh",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

execution_date = '{{ ds_nodash }}'

with DAG(
    dag_id="bitbucket_pull_request",
    description="Fetch Bitbucket pull request API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bitbucket", "minio", "api"],
) as dag:

    fetch_pull_request = PythonOperator(
        task_id="fetch_pull_request",
        python_callable=fetch_entity_by_repo,
        op_kwargs={
            "repo_url": "https://api.bitbucket.org/2.0/repositories/gemcorp",
            "entity_type": "pullrequests",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/pull_request/date={execution_date}",
        },
    )

    fetch_pull_request
