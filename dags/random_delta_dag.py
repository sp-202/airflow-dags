from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
import os
import yaml

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'random_delta_ingestion_v2',
    default_args=default_args,
    description='Random Delta Data Generation DAG',
    schedule_interval=None,
    catchup=False,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    # Load the manifest file content
    manifest_path = os.path.join(os.path.dirname(__file__), "random_delta_manifest.yaml")
    with open(manifest_path, 'r') as f:
        manifest = yaml.safe_load(f)

    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file=manifest,
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        }
    )
