from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import pendulum
import os

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
}

with DAG(
    'spark-dri-sales-customer-wise',
    is_paused_upon_creation=False,
    default_args=default_args,
    description='Load DRI sales data customer-wise',
    schedule=None,
    catchup=False,
    max_active_runs=5,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    submit_job = SparkKubernetesOperator(
        task_id='extract_dri_sales_customer_wise',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': "spark-dri-sales-customer-wise",
            'application_file_s3': 's3a://dags/scripts/dri_sales_customer_wise.py'
        }
    )
