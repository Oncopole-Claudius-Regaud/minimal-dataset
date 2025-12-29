import pandas as pd
import sys
import os

# Ajoute la racine du projet au chemin Python
# (Remonte d'un niveau pour sortir de 'utils' et être dans 'minimal_dataset')
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import depuis votre fichier db.py local
from utils.db import connect_to_iris, get_oncopole_hook

def extract_patients():
    """Extraction depuis IRIS"""
    connection = connect_to_iris()
    
    # Chemin absolu vers le SQL pour éviter les erreurs de dossier de travail
    sql_path = os.path.join(project_root, "sql", "extract_bio.sql")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()
    
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def save_patients(df):
    """Sauvegarde locale"""
    output_path = os.path.join(project_root, "data", "patients.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")

def push_to_target_patient(df):
    """Push vers Oncopole"""
    conn = get_oncopole_hook()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE target_patient;")
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO target_patient (patient_id, name, dob) VALUES (%s, %s, %s)",
                (row['patient_id'], row['name'], row['dob'])
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()