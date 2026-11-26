from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import os
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
}

with DAG(
    'spark-dri-ingetsion-watch-v1',
    is_paused_upon_creation=False,
    default_args=default_args,
    description='Data extract from MS-SQL database',
    schedule=None,
    catchup=False,
    max_active_runs=5, # <--- ADDED THIS
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file="spark_dri_ingestion_manifest.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        }
    )
