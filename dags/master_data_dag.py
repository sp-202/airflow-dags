from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import pendulum

# Define the IST timezone
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'master_orchestrator_pipeline_v1',
    default_args=default_args,
    description='Triggers Raw Ingestion and DRI Processing daily at 9, 1, and 5 IST',
    # CRON: 30 3,7,11 * * * translates to 03:30, 07:30, 11:30 UTC
    schedule_interval='30 3,7,11 * * *', 
    catchup=False
) as dag:

    # 1. Trigger the Raw Incremental Load
    trigger_raw = TriggerDagRunOperator(
        task_id='trigger_nav_raw',
        trigger_dag_id='spark-nav-raw-incremental-v1',
        wait_for_completion=True,
        poke_interval=30
    )

    # 2. Trigger the DRI Output Processing
    trigger_dri = TriggerDagRunOperator(
        task_id='trigger_dri_ingestion',
        trigger_dag_id='spark-dri-ingetsion-watch-v1',
        wait_for_completion=True,
        poke_interval=30
    )

    # Dependency: Raw MUST complete successfully before DRI starts
    trigger_raw >> trigger_dri