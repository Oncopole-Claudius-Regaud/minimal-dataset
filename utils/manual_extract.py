import os
import sys
import pandas as pd
from datetime import datetime
from cryptography.fernet import Fernet

# Ajout du chemin pour les imports locaux
sys.path.append(os.getcwd())
from utils.db import get_oncopole_hook

def decrypt_value(value, cipher):
    """Déchiffre si possible, sinon retourne brut"""
    if value is None or str(value).strip() == "":
        return value
    try:
        return cipher.decrypt(str(value).encode()).decode()
    except Exception:
        return value

def main():
    decrypt_flag = os.getenv("DECRYPT_DATA") == "true"
    # Dossier de destination (facilement accessible)
    output_dir = "/home/administrateur/extractions"
    
    # 1. Création du dossier s'il n'existe pas
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # 2. Lecture SQL et Extraction
    sql_file = "sql/extract_data.sql"
    conn = get_oncopole_hook()
    
    with open(sql_file, "r") as f:
        query = f.read()
    
    df = pd.read_sql(query, conn)
    conn.close()

    # 3. Logique de déchiffrement
    if decrypt_flag:
        key = os.getenv("KEY_ENCRYPT_MINIMAL_DATASET")
        if not key:
            print("ERREUR : Secret KEY_ENCRYPT_MINIMAL_DATASET manquant.")
            sys.exit(1)
        
        cipher = Fernet(key.encode())
        # Colonnes à traiter
        cols = ['ipp_ocr', 'ipp_chu', 'gender', 'nom', 'prenom']
        for col in cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: decrypt_value(x, cipher))
        print("--- Données déchiffrées ---")
    else:
        print("--- Données brutes (chiffrées) ---")

    # 4. Sauvegarde CSV avec Date et Heure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "DECRYPTED" if decrypt_flag else "RAW"
    filename = f"extract_patients_{mode}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False, sep=";", encoding="utf-8")
    
    print(f"Extraction terminée avec succès !")
    print(f"Fichier disponible ici : {filepath}")

if __name__ == "__main__":
    main()