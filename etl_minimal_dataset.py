from datetime import datetime
import sys
import os

# --- LOGIQUE D'IMPORTATION ---
# On récupère le dossier où se trouve ce DAG (minimal_dataset)
dag_folder = os.path.dirname(os.path.abspath(__file__))

# On l'ajoute en PRIORITÉ (index 0) pour que 'utils' soit celui du projet
if dag_folder not in sys.path:
    sys.path.insert(0, dag_folder)
# -----------------------------

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.tasks_minimal_dataset import extract_patients, save_patients, push_to_target_patient

def etl_task():
    """Fonction principale de l'ETL"""
    df = extract_patients()
    save_patients(df)
    push_to_target_patient(df)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_minimal_dataset",
    default_args=default_args,
    description="ETL Patients IRIS -> Oncopole",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "minimal_dataset"],
) as dag:

    run_etl = PythonOperator(
        task_id="extract_save_push_patients",
        python_callable=etl_task,
    )