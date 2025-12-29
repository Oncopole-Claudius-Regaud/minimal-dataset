import pandas as pd
import sys
import os

# Ajoute la racine du projet au chemin Python
# (Remonte d'un niveau pour sortir de 'utils' et être dans 'minimal_dataset')
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import depuis votre fichier db.py local
from minimal_dataset.utils.db import connect_to_iris, get_oncopole_hook

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
    """Push vers la table minimal_dataset.patient avec les bonnes colonnes"""
    conn = get_oncopole_hook()
    cursor = conn.cursor()
    try:
        # On vide la table avant insertion
        cursor.execute("TRUNCATE TABLE minimal_dataset.patient;")
        insert_query = """
            INSERT INTO minimal_dataset.patient (
                ipp_ocr, 
                ipp_chu, 
                gender, 
                date_of_death, 
                nom, 
                prenom, 
                date_of_birth, 
                birth_city
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cursor.execute(
                insert_query,
                (
                    row.get('patient_id'),
                    None,                      
                    row.get('gender'),         
                    None,              
                    row.get('name'),           
                    row.get('prenom'),         
                    row.get('dob'),             
                    None                        
                )
            )
        conn.commit()
        print("Insertion réussie dans minimal_dataset.patient")
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de l'insertion : {e}")
        raise e
    finally:
        cursor.close()
        conn.close()