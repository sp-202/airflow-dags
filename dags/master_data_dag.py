from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

# 1. Define the IST timezone
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
}

with DAG(
    'master_orchestrator_pipeline_v1',
    is_paused_upon_creation=False, 
    default_args=default_args,
    description='Triggers Raw Ingestion and DRI Processing at 9, 13, and 17 IST',
    # 2. Use 'schedule' instead of 'schedule_interval' 
    # 3. Use the actual IST hours: 9, 14, and 18
    schedule='0 9,14,18 * * *', 
    max_active_runs=5,
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