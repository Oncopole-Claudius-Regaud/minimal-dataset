import yaml
import pandas as pd
import cx_Oracle
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'DATA-IA',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'etl_parcours_chimio',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL Oracle -> PostgreSQL'
)

def clean_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def extract_and_load():
    import os
    print("[DEBUG] LD_LIBRARY_PATH visible pour Airflow:", os.environ.get("LD_LIBRARY_PATH"))

    # Lecture des credentials depuis le fichier YAML
    with open('/home/administrateur/airflow/dags/credentials.yml', 'r') as file:
        creds = yaml.safe_load(file)

    ora_conf = creds['database']['oracle']
    pg_conf = creds['database']['postgresql']

    # Connexion Oracle
    dsn = ora_conf['dsn']
    user = ora_conf['user']
    password = ora_conf['password']

    import cx_Oracle
    cx_Oracle.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_7")

    conn = cx_Oracle.connect(user, password, dsn, encoding=ora_conf.get('encoding', 'UTF-8'))
    cursor = conn.cursor()

    with open('/home/administrateur/airflow/dags/requete_finale.sql', 'r') as sql_file:
        sql = sql_file.read()

    cursor.execute(sql)
    columns = [col[0].lower() for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    print("[DEBUG] Colonnes réelles depuis Oracle :", df.columns.tolist())
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
    df["nb_cycles"] = df["nb_cycles"].astype("object")
    df = df.where(pd.notnull(df), None)

    cursor.close()
    conn.close()

    # Connexion PostgreSQL (via Airflow's PostgresHook)
    postgres = PostgresHook(postgres_conn_id='postgres_chimio')
    # Récupération des personnes depuis osiris.person
    patients_df = postgres.get_pandas_df("""
        SELECT patient_id, ipp_ocr
        FROM osiris.patient
    """)

    # Merge avec les données Oracle (df)
    df = df.merge(
        patients_df[["patient_id", "ipp_ocr"]],
        how='left',
        left_on='noobspat',
        right_on='ipp_ocr'
    ).rename(columns={"patient_id": "patient_id"})

    df.drop(columns=["ipp_ocr"], inplace=True)
    # Détection des patients non trouvés (sans correspondance dans osiris.person)
    missing = df[df["patient_id"].isnull()]
    if not missing.empty:
        print("[WARNING] Patients non trouvés dans osiris.patient :", missing["noobspat"].tolist())
        missing.to_csv("/mnt/data/patients_non_trouves_treatment_line.csv", index=False)

    # On garde uniquement ceux avec correspondance
    df = df[df["patient_id"].notnull()]


    df = df.drop_duplicates(subset=[
        "patient_id",
        "noobspat",
        "treatment_line_number",
        "treatment_label",
        "treatment_comment",
        "protocol_name",
        "protocol_detail",
        "protocol_category",
        "protocol_type",
        "local_code",
        "valid_protocol",
        "start_date",
        "end_date",
        "nb_cycles",
        "radiation"
    ])
    print("[DEBUG] Colonnes disponibles dans df :", df.columns.tolist())
    # Log debug
    print("[DEBUG] Lignes après merge et déduplication :", df.shape[0])
    print("[DEBUG] Colonnes :", df.columns.tolist())

    print("[DEBUG] Valeurs max cycles N:")

    # Insertion dans PostgreSQL
    for _, row in df.iterrows():
        postgres.run(
            """
            INSERT INTO osiris.treatment_line (
                patient_id,
                noobspat,
                treatment_line_number,
                treatment_label,
                treatment_comment,
                protocol_name,
                protocol_detail,
                protocol_category,
                protocol_type,
                local_code,
                valid_protocol,
                start_date,
                end_date,
                nb_cycles,
                radiation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                row["patient_id"],
                row["noobspat"],
                row["treatment_line_number"],
                row["treatment_label"],
                row["treatment_comment"],
                row.get("protocol_name"),
                row.get("protocol_detail"),
                row.get("protocol_category"),
                row.get("protocol_type"),
                clean_int(row.get("local_code")),
                clean_int(row.get("valid_protocol")),
                row["start_date"],
                row["end_date"],
                row["nb_cycles"],
                row["radiation"]
            )
        )

    # Nouvelle vérification : patients PostgreSQL sans traitement Oracle
    patients_with_treatment = set(df["patient_id"])
    all_patients = set(patients_df["patient_id"])
    patients_without_treatment = all_patients - patients_with_treatment

    patients_missing_df = patients_df[patients_df["patient_id"].isin(patients_without_treatment)]

    if not patients_missing_df.empty:
        print(f"[INFO] {len(patients_missing_df)} patients sans traitement Oracle détectés.")
        patients_missing_df.to_csv("/mnt/data/patients_sans_traitement_oracle.csv", index=False)


etl_task = PythonOperator(
    task_id='etl_direct',
    python_callable=extract_and_load,
    dag=dag
)
