from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minimal_dataset.utils.tasks_minimal_dataset import extract_patients, save_patients, push_to_target_patient

def etl_task():
    df = extract_patients()
    save_patients(df)            # sauvegarde locale
    push_to_target_patient(df)   # push dans Oncopole

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_minimal_dataset",
    default_args=default_args,
    description="ETL minimal dataset - extraction patients IRIS et push Oncopole",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "patients"],
) as dag:

    run_etl = PythonOperator(
        task_id="extract_save_push_patients",
        python_callable=etl_task,
    )
