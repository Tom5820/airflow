import json
import requests
from datetime import datetime
from typing import Optional
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from plugins.utils.get_config import CONFIG


def fetch_api_to_minio(
    api_url: str,
    bucket_name: str,
    aws_conn_id: str = "aws_minio",
    object_prefix: str = "api",
    object_name: Optional[str] = None,
    timeout: int = 30
):
    """
    Generic function:
    Call REST API and store JSON response to MinIO via Airflow S3Hook
    """

    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {CONFIG['bitbucket_api_token']}"
    }

    response = requests.get(
        api_url,
        headers=headers,
        timeout=timeout
    )
    response.raise_for_status()

    data = response.json()

    # 2. Build object name
    if not object_name:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        object_name = f"{object_prefix}/data_{ts}.json"

    json_str = json.dumps(data, ensure_ascii=False, indent=2)

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
