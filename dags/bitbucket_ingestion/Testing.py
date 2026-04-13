from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import fetch_entity_by_repo
from plugins.utils.common.get_config import CONFIG
from plugins.utils.common.spark_client import create_spark_job

START_DATE = datetime.strptime(CONFIG['start_date'], '%Y-%m-%d')

execution_date = '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}'
previous_day   = '{{ macros.ds_add(ds, -1) }}'
current_day    = '{{ ds }}'


default_args = {
    "owner": "lannh",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bitbucket_testing_pull_request",
    description="Fetch previous day pull requests → MinIO → Spark table",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval="@daily",
    catchup=False,
    tags=["testing", "bitbucket", "minio", "api"],
) as dag:

    fetch_testing_pull_request = PythonOperator(
        task_id="fetch_testing_pull_request",
        python_callable=fetch_entity_by_repo,
        op_kwargs={
            "repo_url": f"https://api.bitbucket.org/2.0/repositories/{CONFIG['bitbucket_workspace']}",
            "entity_type": "pullrequests",
            "bucket_name": CONFIG['raw_bucket'],
            "aws_conn_id": "minio_connection",
            "object_prefix": f"{CONFIG['bitbucket_raw_prefix_path']}/testing_pull_request/date={execution_date}",
            "partition_date": execution_date,
            "params": f'state=ALL&pagelen=50&q=updated_on >= "{previous_day}" AND updated_on < "{current_day}"',
        },
    )

    # spark_testing_pull_request = create_spark_job(
    #     task_id="spark_testing_pull_request",
    #     app_name="spark-testing-pull_request",
    #     main_application_file=f"s3a://{CONFIG['spark_code_bucket']}/bitbucket/pull_request_json_extract.py",
    #     arguments=[
    #         "--source_path", f"s3a://{CONFIG['raw_bucket']}/{CONFIG['bitbucket_raw_prefix_path']}/testing_pull_request/date={execution_date}",
    #         "--output_table", "raw_zone.testing_pull_request",
    #     ],
    #     driver_memory="1g",
    #     executor_memory="1g",
    #     executor_instances=2,
    # )

    # fetch_testing_pull_request >> spark_testing_pull_request
    fetch_testing_pull_request
