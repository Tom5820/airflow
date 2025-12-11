# from datetime import datetime, timedelta
# from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
# import os
# from plugins.utils.get_config import CONFIG

# def create_spark_job(
#         task_id: str,
#         app_name: str,
#         py_file: str,
#         arguments: list = None,
#         driver_memory: str = "512m",
#         executor_memory: str = "512m",
#         executor_instances: int = 2,
#         **kwargs
# ):
#     """
#     Usage:
#         job = create_spark_job_with_local_file(
#             task_id='process',
#             app_name='process-data',
#             py_file='scripts/process.py',  # File trong dags/
#             arguments=['--date', '{{ ds }}']
#         )
#     """
#     return SparkKubernetesOperator(
#         task_id=task_id,
#         namespace='spark-cluster',
#         application_file={
#             'apiVersion': 'sparkoperator.k8s.io/v1beta2',
#             'kind': 'SparkApplication',
#             'metadata': {
#                 'name': f'{app_name}-{{{{ ts_nodash | lower }}}}',
#                 'namespace': 'spark-cluster',
#             },
#             'spec': {
#                 'type': 'Python',
#                 'pythonVersion': '3',
#                 'mode': 'cluster',
#                 'image': CONFIG['spark_image'],
#                 'imagePullPolicy': 'IfNotPresent',
#                 'imagePullSecrets': [CONFIG['spark_img_pull_secret']],
#                 'mainApplicationFile': py_file,
#                 'sparkVersion': '3.4.1',
#                 'restartPolicy': {
#                     'type': 'OnFailure',
#                     'onFailureRetries': 3,
#                 },
#                 'driver': {
#                     'cores': 1,
#                     'memory': driver_memory
#                 },
#                 'executor': {
#                     'cores': 1,
#                     'instances': executor_instances,
#                     'memory': executor_memory
#                 },
#                 'sparkConf': {
#                     'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
#                     'spark.hadoop.hive.metastore.warehouse.dir': 's3a://warehouse/',
#                     'spark.hadoop.fs.s3a.endpoint': CONFIG['minio_endpoint'],
#                     'spark.hadoop.fs.s3a.access.key': CONFIG['minio_access_key'],
#                     'spark.hadoop.fs.s3a.secret.key': CONFIG['minio_secret_key'],
#                     'hive.metastore.uris': CONFIG['hive_metastore_uri'],
#                     'spark.hadoop.fs.s3a.path.style.access': 'true',
#                     'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
#                     'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
#                 },
#                 'arguments': arguments or [],
#             },
#         },
#         kubernetes_conn_id='aiqg_kubernetes',
#         **kwargs
#     )