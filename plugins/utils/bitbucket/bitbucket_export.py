import json
import requests
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import Optional, List, Dict

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
def fetch_entity_by_repo(repo_url, entity_type, bucket_name, aws_conn_id, object_prefix ):
    while repo_url:
        repo_data = invoke_bitbucket_http(repo_url)
        for repo in repo_data.get("values", []):
            repo_slug = repo["slug"]
            api_url = f"{repo_url}/{repo_slug}/{entity_type}"
            fetch_api_to_minio(api_url)

    return 1

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