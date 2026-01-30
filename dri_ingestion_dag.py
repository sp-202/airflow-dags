from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime
import os

# Get the directory where THIS dag file is located
DAG_PATH = os.path.dirname(os.path.realpath(__file__))

with DAG(
    dag_id="spark_dri_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_ingestion_job",
        namespace="default",
        # We point to the YAML file synced by git-sync
        application_file=os.path.join(DAG_PATH, "spark-job-definition.yaml"),
        do_xcom_push=True
    )