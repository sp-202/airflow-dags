from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'spark_dri_ingestion_v3',
    default_args=default_args,
    description='Flexible Spark DRI Ingestion DAG',
    schedule_interval=None,
    catchup=False,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    # The manifest 'spark_dri_ingestion_manifest.yaml' now uses:
    # 1. Kubernetes Secret 'spark-s3-credentials' for keys.
    # 2. Kubernetes ConfigMap 'spark-config' for global defaults.
    # 3. Environment variables for dynamic endpoint assignment.
    submit_job = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace='default',
        application_file="spark_dri_ingestion_manifest.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        }
    )
