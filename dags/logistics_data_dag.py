from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
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
        do_xcom_push=True,
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-logistics-incremental-data',
            'application_file_s3': 's3a://dags/scripts/logistics_data.py',
        }
    )

    monitor_closed_trip = SparkKubernetesSensor(
        task_id='monitor_closed_trip_job',
        namespace='default',
        application_name="{{ task_instance.xcom_pull(task_ids='extract_logistics_incremental_data')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
        attach_log=True,
        poke_interval=20,
        timeout=3600,
    )

    # -------------------------------------------------------------------------
    # JOB 2 — Active/Open trips  (logistics_live_data.py)
    # -------------------------------------------------------------------------
    logistics_current_trip = SparkKubernetesOperator(
        task_id='extract_logistics_open_trip',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        do_xcom_push=True,
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-logistics-open-trip-data',
            'application_file_s3': 's3a://dags/scripts/logistics_live_data.py',
        }
    )

    monitor_current_trip = SparkKubernetesSensor(
        task_id='monitor_current_trip_job',
        namespace='default',
        application_name="{{ task_instance.xcom_pull(task_ids='extract_logistics_open_trip')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
        attach_log=True,
        poke_interval=20,
        timeout=3600,
    )


    # -------------------------------------------------------------------------
    # JOB 3 — Vehicle_master scrap  (logistics_vehicle_data.py)
    # -------------------------------------------------------------------------
    logistics_vehicle_master = SparkKubernetesOperator(
        task_id='extract_vehicle_master',
        namespace='default',
        application_file="spark_jinja_template.yaml",
        do_xcom_push=True,
        params={
            's3_endpoint': 'http://minio.default.svc.cluster.local:9000',
            'app_name': 'spark-logistics-vehicle-master-data',
            'application_file_s3': 's3a://dags/scripts/logistics_vehicle_data.py',
        }
    )

    monitor_vehicle_master = SparkKubernetesSensor(
        task_id='monitor_vehicle_master_job',
        namespace='default',
        application_name="{{ task_instance.xcom_pull(task_ids='extract_vehicle_master')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
        attach_log=True,
        poke_interval=20,
        timeout=3600,
    )

    logistics_closed_trip  >> monitor_closed_trip >> logistics_current_trip >> monitor_current_trip >> logistics_vehicle_master >> monitor_vehicle_master