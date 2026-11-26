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
    'logistics-incremental-data-v1',
    is_paused_upon_creation=False,
    default_args=default_args,
    description='Triggers Logistics Entry at every 3 hrs',
    schedule='30 */3 * * *',  # 0:30, 3:30, 6:30, 9:30, 12:30, 15:30, 18:30, 21:30 IST
    catchup=False,
    max_active_runs=5,
    template_searchpath=[os.path.dirname(__file__)],
) as dag:

    # -------------------------------------------------------------------------
    # JOB 1 — Closed/Incremental trips  (logistics_data.py)
    # -------------------------------------------------------------------------
    logistics_closed_trip = SparkKubernetesOperator(
        task_id='extract_logistics_incremental_data',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-logistics-incremental-data',
            'application_file_s3': 's3a://dags/scripts/logistics_data.py',
        }
    )

    # -------------------------------------------------------------------------
    # JOB 2 — Active/Open trips  (logistics_live_data.py)
    # -------------------------------------------------------------------------
    logistics_current_trip = SparkKubernetesOperator(
        task_id='extract_logistics_open_trip',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-logistics-open-trip-data',
            'application_file_s3': 's3a://dags/scripts/logistics_live_data.py',
        }
    )

    # -------------------------------------------------------------------------
    # JOB 3 — Vehicle_master scrap  (logistics_vehicle_data.py)
    # -------------------------------------------------------------------------
    logistics_vehicle_master = SparkKubernetesOperator(
        task_id='extract_vehicle_master',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-logistics-vehicle-master-data',
            'application_file_s3': 's3a://dags/scripts/logistics_vehicle_data.py',
        }
    )

    logistics_closed_trip >> logistics_current_trip >> logistics_vehicle_master
