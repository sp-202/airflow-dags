from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.bash import BashOperator # FIXED: Added this import
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'spark-dri-ingetsion-watch-v1',
    default_args=default_args,
    description='Data extract from MS-SQL database',
    schedule_interval=None,
    catchup=False,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    # 1. Submit the Job
    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file="spark_dri_ingestion_manifest.yaml",
        do_xcom_push=True,
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        }
    )

    # 2. Wait for the Job AND Fetch Logs
    monitor_job = SparkKubernetesSensor(
        task_id='monitor_random_delta_job',
        namespace='default',
        application_name="{{ task_instance.xcom_pull(task_ids='submit_random_delta_job')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
        attach_log=True,   
        poke_interval=20,
        timeout=3600
    )

    # Set the execution order
    submit_job >> monitor_job 