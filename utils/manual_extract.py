import os
import sys
import pandas as pd
from datetime import datetime
from cryptography.fernet import Fernet

# --- GESTION DES CHEMINS D'IMPORT (DOIT ÊTRE AVANT LES IMPORTS LOCAUX) ---
# 1. Chemin absolu du script actuel (dans .../minimal_dataset/utils/)
current_script_path = os.path.abspath(__file__)

# 2. On remonte d'un niveau -> dossier 'utils'
utils_dir = os.path.dirname(current_script_path)

# 3. On remonte encore d'un niveau -> dossier 'minimal_dataset'
minimal_dataset_dir = os.path.dirname(utils_dir)

# 4. On remonte ENCORE d'un niveau -> RACINE du projet (là où se trouve le runner)
# C'est ce chemin qui permet de faire "from minimal_dataset.utils..."
project_root = os.path.dirname(minimal_dataset_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- MAINTENANT ON PEUT IMPORTER LES MODULES LOCAUX ---
try:
    from minimal_dataset.utils.db import get_oncopole_hook
except ModuleNotFoundError as e:
    print(f"Erreur d'importation. Project Root: {project_root}")
    print(f"Contenu de sys.path: {sys.path}")
    raise e

def decrypt_value(value, cipher):
    """Déchiffre si possible, sinon retourne brut"""
    if value is None or str(value).strip() == "" or str(value).lower() == "none":
        return value
    try:
        # Les valeurs chiffrées Fernet commencent généralement par 'gAAAAA'
        return cipher.decrypt(str(value).encode()).decode()
    except Exception:
        # Si ce n'est pas du Fernet ou que la clé est mauvaise, on rend la valeur brute
        return value

def main():
    # Récupération des paramètres environnement GitHub Actions
    decrypt_flag = os.getenv("DECRYPT_DATA") == "true"
    output_dir = "/home/administrateur/extractions"
    
    # Sécurité dossier de sortie
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Chemin absolu vers le SQL d'extraction
    sql_file = os.path.join(minimal_dataset_dir, "sql", "extract_data.sql")
    
    print(f"Tentative de lecture SQL : {sql_file}")
    
    # Extraction
    conn = get_oncopole_hook()
    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            query = f.read()
        
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    # Logique de déchiffrement
    if decrypt_flag:
        key = os.getenv("KEY_ENCRYPT_MINIMAL_DATASET")
        if not key:
            print("ERREUR : Secret GitHub 'KEY_ENCRYPT_MINIMAL_DATASET' manquant.")
            sys.exit(1)
        
        cipher = Fernet(key.encode())
        # Liste des colonnes sensibles à traiter (basée sur votre table)
        cols_to_process = ['ipp_ocr', 'ipp_chu', 'gender', 'nom', 'prenom']
        
        for col in cols_to_process:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: decrypt_value(x, cipher))
        
        print("--- Mode : Déchiffrement activé ---")
    else:
        print("--- Mode : Données brutes (chiffrées) ---")

    # Sauvegarde CSV avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    status = "DECRYPTED" if decrypt_flag else "RAW"
    filename = f"extract_{status}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Enregistrement (Séparateur point-virgule pour Excel France)
    df.to_csv(filepath, index=False, sep=";", encoding="utf-8")
    
    print(f"Succès ! Fichier généré : {filepath}")
    print(f"Nombre de lignes : {len(df)}")

if __name__ == "__main__":
    main()