from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'master_orchestrator_pipeline_v1',
    is_paused_upon_creation=False,
    default_args=default_args,
    schedule_interval=None, # Changed to None for manual testing
    catchup=False
) as dag:

    trigger_raw = TriggerDagRunOperator(
        task_id='trigger_nav_raw',
        trigger_dag_id='spark-nav-raw-incremental-v1',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True, # Crucial: Allows re-running the same dag
    )

    trigger_dri = TriggerDagRunOperator(
        task_id='trigger_dri_ingestion',
        trigger_dag_id='spark-dri-ingetsion-watch-v1',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_raw >> trigger_dri