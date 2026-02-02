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
    'random_delta_ingestion_v2',
    default_args=default_args,
    description='Random Delta Data Generation DAG',
    schedule_interval=None,
    catchup=False,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    # Dynamically detect the Python file in the PVC
    dags_repo = '/opt/airflow/dags/repo/scripts'
    script_file = None
    for f in os.listdir(dags_repo):
        if f.startswith("random_delta_gen") and f.endswith(".py"):
            script_file = f
            break

    if not script_file:
        raise FileNotFoundError(f"No script found in {dags_repo}")

    full_path = f"local:///{dags_repo}/{script_file}"

    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file="random_delta_manifest.yaml",
        params={
            'main_application_file': full_path,
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        }
    )
