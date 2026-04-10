from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
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
    dag_id="bitbucket_commit",
    description="Fetch Bitbucket pull request API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bitbucket", "minio", "api"],
) as dag:

    fetch_commit = PythonOperator(
        task_id="fetch_commit",
        python_callable=fetch_entity_by_repo,
        op_kwargs={
            "repo_url": f"https://api.bitbucket.org/2.0/repositories/{CONFIG['bitbucket_workspace']}",
            "entity_type": "commits",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/commit/date={execution_date}",
            "partition_date": execution_date,
            "params": "pagelen=50",
        },
    )

    spark_commit_json_extract = create_spark_job(
        task_id="spark_bitbucket_commit_json_extract",
        app_name="spark-bitbucket-commit",
        main_application_file=f"s3a://{CONFIG['spark_code_bucket']}/bitbucket/commit_json_extract.py",
        arguments=["--source_path", f"s3a://{CONFIG['raw_bucket']}/{CONFIG['bitbucket_raw_prefix_path']}/commit/date={execution_date}",
                    "--output_table", "raw_zone.bitbucket_commit"],
        driver_memory="1g",
        executor_memory="1g",
        executor_instances=2
    )

    fetch_commit >> spark_commit_json_extract
