from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'spark_dri_ingestion',
    default_args=default_args,
    description='Spark DRI Ingestion DAG',
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_job = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace='default',
        application_file="local:///opt/spark/scripts/ingest_dri_data.py", # This is where we will mount the script potentially, OR we define the spec inline
        # Actually, SparkKubernetesOperator typically takes an 'application_file' which is a YAML or JSON definition of the SparkApplication.
        # However, for cleaner code, we can define the application via a dictionary in the 'application_file' argument if it supports it, or use 'yaml_file'
        # Wait, the modern operator uses 'application_file' (yaml) or 'application_spec' (dict).
        # Let's use a dictionary to be dynamic and avoid another file.
        # But wait, the user wants to use the script I just put in 'scripts/'.
        # Airflow git-sync puts the repo at /opt/airflow/dags/repo.
        # The Spark Operator creates a Driver Pod. The Driver Pod needs the script.
        # We need to serve the script to the Driver.
        # Option 1: Build a custom image (Too slow).
        # Option 2: Mount the script via ConfigMap (What we did before).
        # Option 3: Host the script on S3/MinIO (Best practice but extra step).
        # Option 4: "local://" implies it's in the image.
        # 
        # The user said "there is no that type of liken in script file in airflow-dags folder inside scripts go and write a new python file and proper airflow dah for that".
        # This implies they want the DAG to use the script from the airflow-dags repo.
        # 
        # CAUTION: The Spark Driver runs as a separate Pod. It does NOT have access to /opt/airflow/dags/repo unless we mount the same PVC.
        # We ALREADY have a shared PVC 'airflow-dags-shared-pvc'.
        # We can mount this PVC to the Driver Pod!
        # Path on Driver: /opt/spark/scripts/ingest_dri_data.py
        # Path on PVC: /dags/scripts/ingest_dri_data.py (Since repo is checked out to <root>/repo usually, but we set --dest=repo? No, we set --root=/dags --dest=repo. So it is /dags/repo/scripts/...)
        
        application_file={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": "spark-dri-ingestion-{{ ts_nodash }}",
                "namespace": "default"
            },
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "image": "subhodeep2022/spark-bigdata:spark-4.0.1-uc-0.3.1-fix-v4-mssql-opt",
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": "local:///opt/spark/repo/scripts/ingest_dri_data.py", # Mounted path
                "sparkVersion": "4.0.1",
                "restartPolicy": {
                    "type": "Never"
                },
                "volumes": [
                    {
                        "name": "dags-volume",
                        "persistentVolumeClaim": {
                            "claimName": "airflow-dags-shared-pvc" # Using the shared PVC we created
                        }
                    },
                    {
                        "name": "spark-config-volume",
                        "configMap": {
                            "name": "spark-config-6ctg8hb4fd" # The one we found
                        }
                    }
                ],
                "driver": {
                    "cores": 1,
                    "coreLimit": "1200m",
                    "memory": "2048m",
                    "labels": {
                        "version": "4.0.1"
                    },
                    "serviceAccount": "spark-operator-spark",
                    "volumeMounts": [
                        {
                            "name": "dags-volume",
                            "mountPath": "/opt/spark/repo" # Mount the whole repo here
                        },
                        {
                            "name": "spark-config-volume",
                            "mountPath": "/opt/spark/conf" # Mount to overwrite defaults
                        }
                    ],
                    "env": [
                         {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
                         {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadmin"}
                    ]
                },
                "executor": {
                    "cores": 1,
                    "instances": 1,
                    "memory": "12000m",
                    "labels": {
                        "version": "4.0.1"
                    },
                    "volumeMounts": [
                        {
                            "name": "spark-config-volume",
                            "mountPath": "/opt/spark/conf"
                        }
                        # Executors don't strictly need the script, but good practice to keep symmetrical if needed
                    ],
                    "env": [
                         {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
                         {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadmin"}
                    ]
                }
            }
        }
    )
