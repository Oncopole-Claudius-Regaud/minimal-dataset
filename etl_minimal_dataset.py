from datetime import datetime
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# AJOUT CRUCIAL : On force Python à regarder dans le dossier courant
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import simplifié (maintenant que le dossier est dans le PATH)
# Note : On cherche le fichier dans le sous-dossier utils du projet
from utils.tasks_minimal_dataset import extract_patients, save_patients, push_to_target_patient

def etl_task():
    df = extract_patients()
    save_patients(df)
    push_to_target_patient(df)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="etl_minimal_dataset",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "patients"],
) as dag:

    run_etl = PythonOperator(
        task_id="extract_save_push_patients",
        python_callable=etl_task,
    )