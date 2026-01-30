from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -----------------------------------
# Default arguments
# -----------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -----------------------------------
# Python callable
# -----------------------------------
def extract_transform(**context):
    print("Extracting data...")
    print("Transforming data...")
    return "ETL Step Completed"

# -----------------------------------
# DAG definition
# -----------------------------------
with DAG(
    dag_id="sample_etl_dag",
    default_args=default_args,
    description="Sample ETL DAG",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "etl"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'DAG started'",
    )

    etl_task = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
        provide_context=True,
    )

    load = BashOperator(
        task_id="load_data",
        bash_command="echo 'Loading data into target system'",
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'DAG finished'",
    )

    # -----------------------------------
    # Task dependencies
    # -----------------------------------
    start >> etl_task >> load >> end
