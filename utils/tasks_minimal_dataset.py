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
    """Push vers minimal_dataset.patient en utilisant les alias SQL"""
    conn = get_oncopole_hook()
    cursor = conn.cursor()
    try:
        # Vidage de la table avant insertion
        cursor.execute("TRUNCATE TABLE minimal_dataset.patient;")
        
        # Requête d'insertion vers PostgreSQL
        # Note : les noms de colonnes à gauche sont ceux de la table Postgres
        insert_query = """
            INSERT INTO minimal_dataset.patient (
                ipp_ocr, ipp_chu, gender, date_of_death, 
                nom, prenom, date_of_birth, birth_city
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            # ICI : Les noms dans row.get() correspondent EXACTEMENT à vos alias SQL
            values = (
                row.get('ipp_ocr'),       
                row.get('ipp_chu'),       
                row.get('gender'),        
                row.get('date_of_death'), 
                row.get('nom'),           
                row.get('prenom'),        
                row.get('date_of_birth'), 
                row.get('birth_city')
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        print(f"Succès : {len(df)} lignes insérées avec les alias corrects.")
        
    except Exception as e:
        conn.rollback()
        print(f"Erreur d'insertion : {e}")
        raise e
    finally:
        cursor.close()
        conn.close()