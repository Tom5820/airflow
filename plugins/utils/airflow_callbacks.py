import requests
from airflow.utils.state import State

def dag_failure_callback(context):
    """
    Callback khi DAG failed
    - Thu thập danh sách task_id bị failed
    - Gọi API notify
    """

    dag_run = context.get("dag_run")
    dag = context.get("dag")

    failed_tasks = []

    if dag_run:
        for ti in dag_run.get_task_instances():
            if ti.state == State.FAILED:
                failed_tasks.append({
                    "task_id": ti.task_id,
                    "try_number": ti.try_number,
                    "log_url": ti.log_url,
                })

    payload = {
        "dag_id": dag.dag_id if dag else None,
        "run_id": dag_run.run_id if dag_run else None,
        "execution_date": str(context.get("execution_date")),
        "failed_tasks": failed_tasks,
        "exception": str(context.get("exception")),
    }

    try:
        requests.post(
            url="http://172.16.13.59:8585/airflow_callback",
            json=payload,
            timeout=10,
        )
    except Exception as e:
        # Không raise exception để tránh làm hỏng scheduler
        print(f"[DAG FAILURE CALLBACK] Error calling API: {e}")
