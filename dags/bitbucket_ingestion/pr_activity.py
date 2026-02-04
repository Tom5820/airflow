from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import fetch_pr_activity, fetch_entity_by_repo
from plugins.utils.common.get_config import CONFIG
from plugins.utils.common.spark_client import create_spark_job


default_args = {
    "owner": "lannh",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

execution_date = '{{ ds_nodash }}'

with DAG(
    dag_id="bitbucket_pr_activity",
    description="Fetch Bitbucket Pull Request Activity API and store raw JSON to MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # schedule="@daily",
    catchup=False,
    tags=["bitbucket", "minio", "api", "activity"],
) as dag:

    # fetch_pr_activity = PythonOperator(
    #     task_id="fetch_pr_activity",
    #     python_callable=fetch_pr_activity,
    #     op_kwargs={
    #         "repo_url": f"https://api.bitbucket.org/2.0/repositories/{CONFIG['bitbucket_workspace']}",
    #         "bucket_name": CONFIG['raw_bucket'],
    #         "aws_conn_id": "minio_connection",
    #         "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/pr_activity/date={execution_date}",
    #         "partition_date": execution_date,
    #     },
    # )

    fetch_pull_request = PythonOperator(
        task_id="fetch_pull_request",
        python_callable=fetch_entity_by_repo,
        op_kwargs={
            "repo_url": f"https://api.bitbucket.org/2.0/repositories/{CONFIG['bitbucket_workspace']}",
            "entity_type": "pullrequests/activity",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/pr_activity/date={execution_date}",
            "partition_date": execution_date,
        },
    )

    spark_pr_activity_json_extract = create_spark_job(
        task_id="spark_bitbucket_pr_activity_json_extract",
        app_name="spark-bitbucket-pr_activity",
        main_application_file=f"s3a://{CONFIG['spark_code_bucket']}/bitbucket/pr_activity_json_extract.py",
        arguments=["--source_path", f"s3a://{CONFIG['raw_bucket']}/{CONFIG['bitbucket_raw_prefix_path']}/pr_activity/date={execution_date}",
                    "--output_table", "raw_zone.bitbucket_pull_request_activity"],
        driver_memory="1g",
        executor_memory="1g",
        executor_instances=2
    )

    fetch_pr_activity >> spark_pr_activity_json_extract
