import requests
from airflow.utils.state import State
from airflow.models import TaskInstance


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
        # Sử dụng session để query task instances
        from airflow.settings import Session
        session = Session()
        
        try:
            # Query task instances từ database
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.run_id == dag_run.run_id,
                TaskInstance.state == State.FAILED
            ).all()
            
            for ti in task_instances:
                failed_tasks.append({
                    "task_id": ti.task_id,
                    "try_number": ti.try_number,
                    "log_url": ti.log_url,
                    "start_date": str(ti.start_date) if ti.start_date else None,
                    "end_date": str(ti.end_date) if ti.end_date else None,
                    "duration": ti.duration,
                })
        finally:
            session.close()
    
    payload = {
        "dag_id": dag.dag_id if dag else None,
        "run_id": dag_run.run_id if dag_run else None,
        "execution_date": str(context.get("execution_date")),
        "failed_at": str(context.get("logical_date")) if context.get("logical_date") else str(context.get("execution_date")),
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