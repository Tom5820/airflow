"""
DAG to initialize repos for Bitbucket ingestion state tracking.
This DAG fetches all repos from Bitbucket and saves them to PostgreSQL for tracking.
Should run before fetch DAGs or as upstream dependency.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.utils.bitbucket.bitbucket_export import invoke_bitbucket_http
from plugins.utils.bitbucket.ingestion_state import IngestionStateManager
from plugins.utils.common.get_config import CONFIG


def init_repos_for_entity(partition_date: str, entity_type: str, postgres_conn_id: str = "postgres_default"):
    """
    Fetch all repos from Bitbucket and initialize them in the state table.
    
    Args:
        partition_date: Execution date in YYYYMMDD format
        entity_type: Type of entity (commits, pullrequests)
        postgres_conn_id: PostgreSQL connection ID
    """
    workspace = CONFIG['bitbucket_workspace']
    repo_url = f"https://api.bitbucket.org/2.0/repositories/{workspace}"
    
    repo_slugs = []
    current_url = repo_url
    while current_url:
        repo_data = invoke_bitbucket_http(current_url)
        for repo in repo_data.get("values", []):
            repo_slugs.append(repo["slug"])
        current_url = repo_data.get("next")
    
    state_manager = IngestionStateManager(postgres_conn_id=postgres_conn_id)
    state_manager.ensure_table_exists()
    
    count = state_manager.init_repos_for_partition(
        partition_date=partition_date,
        entity_type=entity_type,
        repo_slugs=repo_slugs
    )
    
    return {
        "partition_date": partition_date,
        "entity_type": entity_type,
        "repos_initialized": count,
        "total_repos": len(repo_slugs)
    }


default_args = {
    "owner": "lannh",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

execution_date = '{{ ds_nodash }}'

with DAG(
    dag_id="bitbucket_init_ingestion_state",
    description="Initialize Bitbucket repos for ingestion tracking",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bitbucket", "ingestion", "init"],
) as dag:

    init_commits = PythonOperator(
        task_id="init_commits_state",
        python_callable=init_repos_for_entity,
        op_kwargs={
            "partition_date": execution_date,
            "entity_type": "commits",
        },
    )

    init_pullrequests = PythonOperator(
        task_id="init_pullrequests_state",
        python_callable=init_repos_for_entity,
        op_kwargs={
            "partition_date": execution_date,
            "entity_type": "pullrequests",
        },
    )

    init_pq_activity = PythonOperator(
        task_id="init_pq_activity_state",
        python_callable=init_repos_for_entity,
        op_kwargs={
            "partition_date": execution_date,
            "entity_type": "pullrequests_activity",
        },
    )

    [init_commits, init_pullrequests, init_pq_activity]
