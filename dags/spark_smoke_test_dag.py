"""Manual health check for Airflow -> Spark operator -> Spark -> Hive metastore/MinIO. Writes nothing."""
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import os
import pendulum

with DAG(
    'airflow3-spark-smoke-test',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    description='Read-only Spark smoke test (spark.range count + SHOW DATABASES)',
    template_searchpath=[os.path.dirname(__file__)],
) as dag:
    SparkKubernetesOperator(
        task_id='spark_smoke_test',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-smoke-test',
            'application_file_s3': 's3a://dags/tests/spark_smoke_test.py',
        },
    )
