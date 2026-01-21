from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import fetch_entity_by_repo
from plugins.utils.common.get_config import CONFIG
from plugins.utils.common.spark_client import create_spark_job



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
            "repo_url": f"https://api.bitbucket.org/2.0/repositories/{CONFIG['bitbucket_workspace']}",
            "entity_type": "pullrequests",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/pull_request/date={execution_date}",
        },
    )

    spark_pull_request_json_extract = create_spark_job(
        task_id="spark_bitbucket_pull_request_json_extract",
        app_name="spark-bitbucket-pull_request",
        main_application_file=f"s3a://{CONFIG['spark_code_bucket']}/bitbucket/pull_request_json_extract.py",
        arguments=["--source_path", f"s3a://{CONFIG['raw_bucket']}/{CONFIG['bitbucket_raw_prefix_path']}/pull_request/date={execution_date}",
                    "--output_table", "raw_zone.bitbucket_pull_request"],
        driver_memory="1g",
        executor_memory="1g",
        executor_instances=2
    )

    fetch_pull_request
