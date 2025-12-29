import pandas as pd
import sys
import os
from cryptography.fernet import Fernet
from airflow.hooks.base import BaseHook

# --- CONFIGURATION DU CHEMIN PROJET ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import depuis votre utilitaire de base de données local
from minimal_dataset.utils.db import connect_to_iris, get_oncopole_hook

def encrypt_value(value, cipher):
    """
    Chiffre une valeur en utilisant le moteur cipher fourni.
    Retourne None si la valeur est vide pour éviter de chiffrer du 'vide'.
    """
    if value is None or str(value).strip().lower() in ["none", ""]:
        return None
    # Encodage en bytes, chiffrement, puis retour en chaîne de caractères
    return cipher.encrypt(str(value).encode()).decode()

def extract_patients():
    """Extraction depuis IRIS en utilisant le fichier SQL externe"""
    connection = connect_to_iris()
    
    # Construction du chemin absolu vers le fichier SQL
    sql_path = os.path.join(project_root, "sql", "extract_bio.sql")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()
    
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def save_patients(df):
    """Sauvegarde de sécurité du DataFrame en CSV local"""
    output_path = os.path.join(project_root, "data", "patients.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")

def push_to_target_patient(df):
    """
    Chiffre les données sensibles et les insère dans PostgreSQL.
    La clé est récupérée dans le champ PASSWORD de la connexion Airflow.
    """
    
    # 1. RÉCUPÉRATION DE LA CLÉ DE CHIFFREMENT
    try:
        # On utilise le Conn ID que vous avez créé dans Airflow
        conn_info = BaseHook.get_connection("key_encrypt_minimal_dataset")
        encryption_key = conn_info.password
        
        if not encryption_key:
            raise ValueError("Le champ Password de la connexion 'key_encrypt_minimal_dataset' est vide.")
            
        cipher = Fernet(encryption_key.encode())
    except Exception as e:
        print(f"Erreur lors de l'initialisation du chiffrement : {e}")
        raise e

    # 2. CONNEXION À LA BASE CIBLE
    conn = get_oncopole_hook()
    cursor = conn.cursor()
    
    try:
        # Vidage de la table pour le POC
        cursor.execute("TRUNCATE TABLE minimal_dataset.patient;")
        
        # Requête d'insertion respectant les colonnes de votre table Postgres
        insert_query = """
            INSERT INTO minimal_dataset.patient (
                ipp_ocr, ipp_chu, gender, date_of_death, 
                nom, prenom, date_of_birth, birth_city
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            # 3. CHIFFREMENT ET MAPPING
            # On applique encrypt_value sur les alias définis dans votre SQL (ipp_ocr, nom, etc.)
            values = (
                encrypt_value(row.get('ipp_ocr'), cipher),       
                encrypt_value(row.get('ipp_chu'), cipher),       
                encrypt_value(row.get('gender'), cipher),        
                encrypt_value(row.get('date_of_death'), cipher), 
                encrypt_value(row.get('nom'), cipher),           
                encrypt_value(row.get('prenom'), cipher),        
                row.get('date_of_birth'), # On garde généralement la date en clair pour les index
                row.get('birth_city')
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        print(f"Succès : {len(df)} lignes chiffrées et insérées.")
        
    except Exception as e:
        conn.rollback()
        print(f"Erreur d'insertion dans PostgreSQL : {e}")
        raise e
    finally:
        cursor.close()
        conn.close()