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
    'spark_dri_ingestion_v2',
    default_args=default_args,
    description='Spark DRI Ingestion DAG',
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_job = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace='default',
        application_file=os.path.join(os.path.dirname(__file__), "spark_dri_ingestion_manifest.yaml"),
    )
