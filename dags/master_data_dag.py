from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
}

# dag_id suffix -> cron (evaluated in IST because start_date is tz-aware)
SCHEDULES = {
    'a': '0 9,14,18 * * *',   # 09:00, 14:00, 18:00
    'b': '45 16 * * *',       # 16:45
}

def trigger(task_id, dag_id):
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

for suffix, cron in SCHEDULES.items():
    dag_id = f'master_orchestrator_pipeline_v1_{suffix}'

    with DAG(
        dag_id,
        is_paused_upon_creation=False,
        default_args=default_args,
        description='Triggers Raw Ingestion and DRI Processing at 9:00, 14:00, 16:45, and 18:00 IST',
        schedule=cron,
        max_active_runs=5,
        catchup=False,
    ) as dag:

        trigger_nav_raw = trigger('trigger_nav_raw', 'spark-nav-raw-incremental-v1')
        trigger_dri = trigger('trigger_dri_ingestion', 'spark-dri-ingetsion-watch-v1')
        trigger_manual_nav_data = trigger('trigger_nav_manual_ingest', 'spark-manual-nav-data-v1')
        trigger_dri_sales_customer_wise = trigger(
            'spark-dri-sales-customer-wise', 'extract_dri_sales_customer_wise'
        )

        trigger_nav_raw >> trigger_dri >> [trigger_manual_nav_data, trigger_dri_sales_customer_wise]

    globals()[dag_id] = dag  # make sure the scheduler discovers each DAG