from typing import Optional, List, Dict, Any
from airflow.exceptions import AirflowException
from plugins.utils.common.get_config import CONFIG
import yaml

def _get_spark_operator_class() -> Any:
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
    return SparkKubernetesOperator


def create_spark_job(
    task_id: str,
    app_name: str,
    main_application_file: str,
    arguments: Optional[List[str]] = None,
    driver_memory: str = "1g",
    executor_memory: str = "2g",
    executor_cores: int = 1,
    executor_instances: int = 2,
    driver_cores: int = 1,
    image: Optional[str] = None,
    namespace: str = "spark-cluster",
    extra_spark_conf: Optional[Dict[str, str]] = None,
    extra_labels: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Any:

    SparkKubernetesOperator = _get_spark_operator_class()

    if not main_application_file.startswith(("local://", "file://", "s3a://", "hdfs://")):
        raise AirflowException("main_application_file must start with local://, file://, s3:// or hdfs://")

    application_name = f"{app_name}-{{{{ ts_nodash.lower() }}}}"

    base_spark_conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": CONFIG["minio_endpoint"],
        "spark.hadoop.fs.s3a.access.key": CONFIG["minio_access_key"],
        "spark.hadoop.fs.s3a.secret.key": CONFIG["minio_secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.jars.ivy": "/tmp/.ivy2"

    }
    if extra_spark_conf:
        base_spark_conf.update(extra_spark_conf)

    image = image or CONFIG.get("spark_image")
    if not image:
        raise AirflowException("spark_image not found in CONFIG")

    application_file = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": application_name,
            "namespace": namespace,
            "labels": {
                "app": app_name,
                "created-by": "airflow",
                "dag_id": "{{ dag.dag_id }}",
                "task_id": task_id,
                **(extra_labels or {}),
            },
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "sparkVersion": "3.4.1",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "imagePullSecrets": [CONFIG["spark_img_pull_secret"]]
            if CONFIG.get("spark_img_pull_secret")
            else [],
            "mainApplicationFile": main_application_file,
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 1,
            },
            "driver": {
                "serviceAccount": CONFIG["spark_operator_SA"],
                "cores": driver_cores,
                "memory": driver_memory
            },
            "executor": {
                "serviceAccount": CONFIG["spark_operator_SA"],
                "cores": executor_cores,
                "instances": executor_instances,
                "memory": executor_memory,
            },
            "sparkConf": base_spark_conf,
            "arguments": arguments or []
        },
    }

    print(application_file)
    application_file_yaml = yaml.safe_dump(application_file, sort_keys=False, allow_unicode=True)
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=namespace,
        application_file=application_file_yaml,
        kubernetes_conn_id="aiqg_kubernetes",
        do_xcom_push=False,
        **kwargs,
    )