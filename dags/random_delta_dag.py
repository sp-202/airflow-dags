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
    dag_id='random_delta_ingestion_v2',
    default_args=default_args,
    description='Random Delta Data Generation DAG',
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/repo/dags'],  # YAML lives here
) as dag:

    # 🔍 Dynamically detect the script inside PVC
    scripts_dir = '/opt/airflow/dags/repo/scripts'
    script_file = None

    for f in os.listdir(scripts_dir):
        if f.startswith("random_delta_gen") and f.endswith(".py"):
            script_file = f
            break

    if not script_file:
        raise FileNotFoundError(
            f"No random_delta_gen*.py found in {scripts_dir}"
        )

    main_app_path = f"local:///{scripts_dir}/{script_file}"

    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file='random_delta_manifest.yaml',  # existing file
        params={
            'main_application_file': main_app_path,
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000'
        },
        do_xcom_push=False
    )
