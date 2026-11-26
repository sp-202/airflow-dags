from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import os
import pendulum
import yaml

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'random_delta_ingestion_v2',
    default_args=default_args,
    description='Random Delta Data Generation DAG',
    schedule=None,
    catchup=False,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file="random_delta_manifest.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        }
    )
