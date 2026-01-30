from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime
import os

# Get the directory where THIS dag file is located
# Force the path to the stable symlink repo path
REPO_PATH = "/opt/airflow/dags/dags-v5/repo"

with DAG(
    dag_id="spark_dri_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_ingestion_job",
        namespace="default",
        # Use the explicit path here
        application_file=os.path.join(REPO_PATH, "spark-job-definition.yaml"),
        do_xcom_push=True
    )