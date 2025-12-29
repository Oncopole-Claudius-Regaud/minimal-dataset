import pandas as pd
from utils.db import connect_to_iris, get_oncopole_hook

def extract_patients():
    """
    Extrait les patients depuis IRIS via la requête SQL.
    """
    connection = connect_to_iris()
    
    with open("sql/extract_bio.sql", "r", encoding="utf-8") as f:
        query = f.read()
    
    df = pd.read_sql(query, connection)
    connection.close()
    
    return df

def save_patients(df, output_path="data/patients.csv"):
    """
    Sauvegarde le DataFrame dans un CSV local.
    """
    df.to_csv(output_path, index=False, encoding="utf-8")


def push_to_target_patient(df):
    """
    Truncate la table target_patient et pousse le DataFrame dans Oncopole.
    """
    conn = get_oncopole_hook()
    cursor = conn.cursor()

    # Truncate table avant insertion
    cursor.execute("TRUNCATE TABLE target_patient;")

    # Insertion des données
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO target_patient (patient_id, name, dob)
            VALUES (%s, %s, %s)
            """,
            (row['patient_id'], row['name'], row['dob'])
        )

    conn.commit()
    cursor.close()
    conn.close()
