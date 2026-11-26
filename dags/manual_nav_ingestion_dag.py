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
    'spark-manual-nav-data-v1',
    is_paused_upon_creation=False,
    default_args=default_args,
    description='Load incremental data extract from MS-SQL database',
    schedule=None,
    catchup=False,
    max_active_runs=5, # <--- ADDED THIS
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    submit_job = SparkKubernetesOperator(
        task_id='extract_incremantal_data',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': "spark-manual-nav-data",
            'application_file_s3': 's3a://dags/scripts/ingest_manual_entry.py'
        }
    )
