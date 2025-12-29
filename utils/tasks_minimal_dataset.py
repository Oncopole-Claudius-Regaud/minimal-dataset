import pandas as pd
import sys
import os
from cryptography.fernet import Fernet
from airflow.hooks.base import BaseHook

# Configuration des chemins pour l'import local
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from minimal_dataset.utils.db import connect_to_iris, get_oncopole_hook

def encrypt_value(value, cipher):
    """Chiffre une valeur en string. Retourne None si la valeur est vide."""
    if value is None or str(value).strip() == "" or str(value).lower() == "none":
        return None
    # Encodage en bytes puis chiffrement et retour en string
    return cipher.encrypt(str(value).encode()).decode()

def push_to_target_patient(df):
    """
    Push vers minimal_dataset.patient avec chiffrement.
    La clé est récupérée dans le champ 'Password' de la connexion 'key_encrypt_minimal_dataset'.
    """
    
    # 1. Récupération de la clé via BaseHook
    try:
        # On récupère l'objet connexion complet
        conn_config = BaseHook.get_connection("key_encrypt_minimal_dataset")
        encryption_key = conn_config.password
        
        if not encryption_key:
            raise ValueError("Le champ Password de la connexion est vide.")
            
        cipher = Fernet(encryption_key.encode())
    except Exception as e:
        print(f"Erreur lors de la récupération de la clé de chiffrement : {e}")
        raise e

    # Connexion à la base cible
    conn = get_oncopole_hook()
    cursor = conn.cursor()
    
    try:
        # Vidage de la table avant insertion
        cursor.execute("TRUNCATE TABLE minimal_dataset.patient;")
        
        insert_query = """
            INSERT INTO minimal_dataset.patient (
                ipp_ocr, ipp_chu, gender, date_of_death, 
                nom, prenom, date_of_birth, birth_city
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            # 2. Chiffrement des colonnes sensibles avant insertion
            # On applique encrypt_value sur les alias de votre requête SQL
            values = (
                encrypt_value(row.get('ipp_ocr'), cipher),       
                encrypt_value(row.get('ipp_chu'), cipher),       
                encrypt_value(row.get('gender'), cipher),        
                encrypt_value(row.get('date_of_death'), cipher), 
                encrypt_value(row.get('nom'), cipher),           
                encrypt_value(row.get('prenom'), cipher),        
                row.get('date_of_birth'), # Date brute (Postgres gère le format DATE)
                row.get('birth_city')
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        print(f"Succès : {len(df)} lignes chiffrées et insérées dans PostgreSQL.")
        
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de l'insertion : {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

# Gardez vos fonctions extract_patients et save_patients telles quelles