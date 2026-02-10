from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

# Define the IST timezone
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # FIXED: Changed 'tzinfo' to 'tz'
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
}

with DAG(
    'master_orchestrator_pipeline_v1',
    is_paused_upon_creation=False, 
    default_args=default_args,
    description='Triggers Raw Ingestion and DRI Processing at 9, 13, and 17 IST',
    # 03:30, 07:30, 11:30 UTC = 09:00, 13:00, 17:00 IST
    schedule_interval='30 3,7,11 * * *', 
    max_active_runs=5, # <--- ADDED THIS
    catchup=False
) as dag:

    trigger_raw = TriggerDagRunOperator(
        task_id='trigger_nav_raw',
        trigger_dag_id='spark-nav-raw-incremental-v1',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_dri = TriggerDagRunOperator(
        task_id='trigger_dri_ingestion',
        trigger_dag_id='spark-dri-ingetsion-watch-v1',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_raw >> trigger_dri