from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import pendulum

# Define the IST timezone
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Using the local timezone for the start date ensures predictable scheduling
    'start_date': pendulum.datetime(2024, 1, 1, tzinfo=local_tz),
}

with DAG(
    'master_orchestrator_pipeline_v1',
    is_paused_upon_creation=False, # Auto-activates the DAG
    default_args=default_args,
    description='Triggers Raw Ingestion and DRI Processing at 9, 13, and 17 IST',
    # 03:30, 07:30, 11:30 UTC translates to 09:00, 13:00, 17:00 IST
    schedule_interval='30 3,7,11 * * *', 
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

    # Dependency: Job 1 finishes, releases resources, then Job 2 starts
    trigger_raw >> trigger_dri