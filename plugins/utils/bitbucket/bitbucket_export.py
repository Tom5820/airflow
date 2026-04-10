import json
import requests
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import Optional, List, Dict
from airflow.models import Variable
from plugins.utils.common.get_config import CONFIG


def fetch_api_to_minio(
    api_url: str,
    bucket_name: str,
    aws_conn_id: str,
    object_prefix: str,
    object_name: Optional[str] = None,
):
    """
    Generic function:
    Call REST API and store JSON response to MinIO via Airflow S3Hook
    """
    all_values: List[Dict] = []
    current_url = api_url
    meta = {}

    while current_url:
        data = invoke_bitbucket_http(current_url)
        all_values.extend(data.get("values", []))

        if not meta:
            meta = {
                "pagelen": data.get("pagelen"),
                "size": data.get("size"),
            }
        current_url = data.get("next")

    final_payload = {
        "meta": meta,
        "count": len(all_values),
        "api_url": api_url,
        "values": all_values
    }

    # 3. Object name
    if not object_name:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        object_name = f"{object_prefix}/data_{ts}.json"

    json_str = json.dumps(final_payload, ensure_ascii=False, indent=2)

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    s3_hook.load_string(
        string_data=json_str,
        bucket_name=bucket_name,
        key=object_name,
        replace=True
    )

    return {
        "api_url": api_url,
        "bucket": bucket_name,
        "object": object_name,
        "size_bytes": len(json_str)
    }
    
def fetch_entity_by_repo(
    repo_url: str,
    entity_type: str,
    bucket_name: str,
    aws_conn_id: str,
    object_prefix: str,
    partition_date: str,
    postgres_conn_id: str = "postgres_default"
):
    """
    Fetch entity từ Bitbucket với state tracking.
    Chỉ fetch repos chưa completed từ bảng state.
    Khi clear task và chạy lại sẽ chỉ fetch repos còn pending.
    
    Args:
        repo_url: Base URL for repositories API
        entity_type: Type of entity (commits, pullrequests)
        bucket_name: MinIO bucket name
        aws_conn_id: Airflow connection ID for S3/MinIO
        object_prefix: Prefix path for stored objects
        partition_date: Execution date (YYYYMMDD format)
        postgres_conn_id: Airflow connection ID for PostgreSQL
    """
    from plugins.utils.bitbucket.ingestion_state import IngestionStateManager
    
    state_manager = IngestionStateManager(postgres_conn_id=postgres_conn_id)
    
    # if entity_type == "commits":
    #     params = "pagelen=50"
    # elif entity_type == "pullrequests":
    #     params = "state=ALL&pagelen=50"
    # else:
    #     params = "pagelen=50"
    
   
    pending_repos = state_manager.get_pending_repos(partition_date, entity_type)
    
    if not pending_repos:
        summary = state_manager.get_ingestion_summary(partition_date, entity_type)
        return {"status": "no_pending_repos", **summary}

    for repo_slug in pending_repos:
        api_url = f"{repo_url}/{repo_slug}/{entity_type}?{params}"
        try:
            fetch_api_to_minio(api_url, bucket_name, aws_conn_id, object_prefix)
            state_manager.mark_repo_completed(partition_date, entity_type, repo_slug)
        except Exception as e:
            import logging
            logging.error(f"Failed to fetch {repo_slug}: {e}")
            raise 
    summary = state_manager.get_ingestion_summary(partition_date, entity_type)
    return summary


def fetch_pr_activity(
    repo_url: str,
    bucket_name: str,
    aws_conn_id: str,
    object_prefix: str,
    partition_date: str,
    postgres_conn_id: str = "postgres_default"
):
    """
    Fetch PR activity từ Bitbucket với state tracking theo repo_slug.
    - Check state bằng entity_type = "pullrequests_activity", repo_slug = tên repo
    - Với mỗi repo pending: fetch tất cả PRs, rồi fetch activity cho từng PR
    - Mark completed sau khi xử lý xong tất cả PRs của repo đó
    
    Args:
        repo_url: Base URL (e.g., https://api.bitbucket.org/2.0/repositories/workspace)
        bucket_name: MinIO bucket name
        aws_conn_id: Airflow connection ID for S3/MinIO
        object_prefix: Prefix path for stored objects
        partition_date: Execution date (YYYYMMDD format)
        postgres_conn_id: Airflow connection ID for PostgreSQL
    """
    from plugins.utils.bitbucket.ingestion_state import IngestionStateManager
    import logging
    
    state_manager = IngestionStateManager(postgres_conn_id=postgres_conn_id)
    entity_type = "pullrequests_activity"
    
    pending_repos = state_manager.get_pending_repos(partition_date, entity_type)
    
    if not pending_repos:
        summary = state_manager.get_ingestion_summary(partition_date, entity_type)
        return {"status": "no_pending_repos", **summary}

    for repo_slug in pending_repos:
        # Step 1: Fetch all PRs for this repo
        pr_url = f"{repo_url}/{repo_slug}/pullrequests?state=ALL&pagelen=50"
        pr_ids = []
        
        while pr_url:
            pr_data = invoke_bitbucket_http(pr_url)
            for pr in pr_data.get("values", []):
                pr_id = pr.get("id")
                if pr_id:
                    pr_ids.append(pr_id)
            pr_url = pr_data.get("next")
        
        # Step 2: Fetch activity for each PR
        for pr_id in pr_ids:
            api_url = f"{repo_url}/{repo_slug}/pullrequests/{pr_id}/activity?pagelen=50"
            try:
                fetch_api_to_minio(api_url, bucket_name, aws_conn_id, object_prefix)
            except Exception as e:
                logging.error(f"Failed to fetch activity for {repo_slug}/PR#{pr_id}: {e}")
                raise
        
        # Step 3: Mark repo as completed after all PRs processed
        state_manager.mark_repo_completed(partition_date, entity_type, repo_slug)
        logging.info(f"Completed {repo_slug}: fetched activity for {len(pr_ids)} PRs")
    
    summary = state_manager.get_ingestion_summary(partition_date, entity_type)
    return summary

def list_repos(workspace: str) -> list[str]:
    """
        Return list of api_url so it can be used directly in expand(api_url=...)
        """
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}"
    api_urls: List[str] = []

    while url:
        data = invoke_bitbucket_http(url)

        for repo in data.get("values", []):
            repo_slug = repo.get("slug")
            if repo_slug:
                api_urls.append(
                    f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_slug}/commits"
                )

        url = data.get("next")
    return api_urls

def invoke_bitbucket_http(url, timeout: int = 30):
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {CONFIG['bitbucket_api_token']}"
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=timeout
    )
    return response.json()
