import os
import sys
import pandas as pd
from cryptography.fernet import Fernet
# On réutilise vos utilitaires existants pour la connexion
sys.path.append(os.getcwd())
from utils.db import get_oncopole_hook

def decrypt_value(value, cipher):
    """Tente de déchiffrer une valeur, sinon retourne la valeur brute"""
    if value is None or str(value).strip() == "":
        return value
    try:
        return cipher.decrypt(str(value).encode()).decode()
    except Exception:
        # Si le déchiffrement échoue (donnée pas chiffrée), on rend la valeur telle quelle
        return value

def main():
    decrypt_flag = os.getenv("DECRYPT_DATA") == "true"
    sql_file = "sql/extract_data.sql"
    
    print(f"Option Déchiffrement : {decrypt_flag}")

    # 1. Connexion et Extraction
    conn = get_oncopole_hook()
    with open(sql_file, "r") as f:
        query = f.read()
    
    df = pd.read_sql(query, conn)
    conn.close()

    # 2. Logique de déchiffrement
    if decrypt_flag:
        key = os.getenv("KEY_ENCRYPT_MINIMAL_DATASET")
        if not key:
            print("Erreur : Clé de déchiffrement manquante dans les secrets.")
            sys.exit(1)
        
        cipher = Fernet(key.encode())
        
        # On applique le déchiffrement sur toutes les colonnes de type 'objet/string'
        # car on ne sait pas à l'avance lesquelles sont chiffrées
        cols_to_decrypt = ['ipp_ocr', 'ipp_chu', 'gender', 'nom', 'prenom']
        for col in cols_to_decrypt:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: decrypt_value(x, cipher))
        print("Données déchiffrées avec succès.")
    else:
        print("Récupération des données brutes (sans déchiffrement).")

    # 3. Affichage ou Sauvegarde
    print(df.head())
    df.to_csv("data/manual_extraction_result.csv", index=False)
    print("Résultat sauvegardé dans data/manual_extraction_result.csv")

if __name__ == "__main__":
    main()