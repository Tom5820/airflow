import requests
from airflow.utils.state import State
from datetime import datetime


def dag_failure_callback(context):
    """
    Callback khi DAG failed
    - Thu thập danh sách task_id bị failed
    - Gọi API notify
    """
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    failed_tasks = []
    
    # Lấy thông tin từ task instance hiện tại (task bị failed)
    if task_instance:
        failed_tasks.append({
            "task_id": task_instance.task_id,
            "try_number": task_instance.try_number,
            "log_url": task_instance.log_url,
            "start_date": str(task_instance.start_date) if task_instance.start_date else None,
            "end_date": str(task_instance.end_date) if task_instance.end_date else None,
            "duration": task_instance.duration,
            "state": str(task_instance.state),
        })
    
    # Thời gian fail
    failed_at = None
    if task_instance and task_instance.end_date:
        failed_at = str(task_instance.end_date)
    elif context.get("logical_date"):
        failed_at = str(context.get("logical_date"))
    else:
        failed_at = str(datetime.now())
    
    payload = {
        "dag_id": dag.dag_id if dag else None,
        "run_id": dag_run.run_id if dag_run else None,
        "execution_date": str(context.get("execution_date")),
        "failed_at": failed_at,
        "failed_tasks": failed_tasks,
        "exception": str(context.get("exception")),
    }
    
    try:
        response = requests.post(
            url="http://172.16.13.59:8585/airflow_callback",
            json=payload,
            timeout=10,
        )
        print(f"[DAG FAILURE CALLBACK] API response: {response.status_code}")
    except Exception as e:
        # Không raise exception để tránh làm hỏng scheduler
        print(f"[DAG FAILURE CALLBACK] Error calling API: {e}")