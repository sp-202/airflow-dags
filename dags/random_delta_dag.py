from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

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

    submit_job = SparkKubernetesOperator(
        task_id='submit_random_delta_job',
        namespace='default',
        application_file='random_delta_manifest.yaml',
        do_xcom_push=False
    )
