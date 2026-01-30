import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

# 1. Define the absolute path to your repo
REPO_PATH = "/opt/airflow/dags/dags-v5/repo"

with DAG(
    dag_id="spark_dri_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    # 2. ADD THIS LINE: This tells Airflow to look here for YAML/JSON files
    template_searchpath=[REPO_PATH] 
) as dag:

    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_ingestion_job",
        namespace="default",
        # 3. Use ONLY the filename here, Jinja will find it in template_searchpath
        application_file="spark-job-definition.yaml",
        do_xcom_push=True
    )