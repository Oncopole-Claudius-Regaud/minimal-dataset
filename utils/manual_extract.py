import os
import sys
import pandas as pd
from datetime import datetime
from cryptography.fernet import Fernet

# --- GESTION DES CHEMINS D'IMPORT ---
# On récupère le chemin absolu du dossier racine du projet
# On remonte de 2 niveaux si le script est dans utils/ (ex: projet/utils/manual_extract.py)
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Maintenant l'import fonctionnera car le dossier parent est dans le PATH
from minimal_dataset.utils.db import get_oncopole_hook

def decrypt_value(value, cipher):
    """Déchiffre si possible, sinon retourne brut"""
    if value is None or str(value).strip() == "" or str(value).lower() == "none":
        return value
    try:
        return cipher.decrypt(str(value).encode()).decode()
    except Exception:
        return value

def main():
    decrypt_flag = os.getenv("DECRYPT_DATA") == "true"
    output_dir = "/home/administrateur/extractions"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Utilisation d'un chemin absolu pour le fichier SQL également
    sql_file = os.path.join(project_root, "minimal_dataset", "sql", "extract_data.sql")
    
    print(f"Tentative de lecture SQL : {sql_file}")
    
    conn = get_oncopole_hook()
    with open(sql_file, "r", encoding="utf-8") as f:
        query = f.read()
    
    df = pd.read_sql(query, conn)
    conn.close()

    if decrypt_flag:
        key = os.getenv("KEY_ENCRYPT_MINIMAL_DATASET")
        if not key:
            print("ERREUR : Secret KEY_ENCRYPT_MINIMAL_DATASET manquant dans GitHub.")
            sys.exit(1)
        
        cipher = Fernet(key.encode())
        cols = ['ipp_ocr', 'ipp_chu', 'gender', 'nom', 'prenom']
        for col in cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: decrypt_value(x, cipher))
        print("--- Données déchiffrées ---")
    else:
        print("--- Données brutes (chiffrées) ---")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "DECRYPTED" if decrypt_flag else "RAW"
    filename = f"extract_patients_{mode}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False, sep=";", encoding="utf-8")
    
    print(f"Extraction terminée avec succès !")
    print(f"Fichier disponible ici : {filepath}")

if __name__ == "__main__":
    main()