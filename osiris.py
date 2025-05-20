import pyodbc
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, time, timedelta
import pyodbc
import psycopg2
from psycopg2.extras import execute_values
import yaml
from dateutil.parser import parse
import os
from collections import defaultdict
import hashlib
from utils.enrich_measurements import enrich_measure_data

# Configuration du logging
logging.basicConfig(
    filename="/home/administrateur/airflow/logs/etl_clickhouse.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Démarrage du DAG Airflow")

# Charger les credentials depuis un fichier YAML
def load_credentials(filename):
    """Chargement des informations d'identification depuis le fichier YAML."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, filename)
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def serialize(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    return obj

def make_serializable(data_list):
    serializable_list = []
    for item in data_list:
        cleaned = {k: serialize(v) for k, v in item.items()}
        serializable_list.append(cleaned)
    return serializable_list

# Fonction d'extraction depuis IRIS
def extract_data_from_iris(**kwargs):
    logging.info("Début de l'extraction des données depuis IRIS")

    try:
        credentials = load_credentials("credentials.yml")
        db_info = credentials['database']

        connection = pyodbc.connect(f"DSN={db_info['dsn']};UID={db_info['username']};PWD={db_info['password']}")
        connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        connection.setencoding(encoding='utf-8')
        cursor = connection.cursor()

        # Requête pour `patient_data_test`
        sql_patient_data = """SELECT DISTINCT
    pat.PAPMI_No AS ipp_ocr,
    pe.PAPER_ResidentNumber AS ipp_chu,
    YEAR(pat.PAPMI_DOB) AS annee_naissance,

    CASE
        WHEN pat.PAPMI_Sex_DR = 2 THEN 'M'
        WHEN pat.PAPMI_Sex_DR = 3 THEN 'F'
        ELSE 'I'
    END AS sexe,

    pat.PAPMI_Deceased_Date AS death_of_death,

    diag.PROB_OnsetDate AS date_diagnostic,

    -- Diagnostic + CIM
    MRC.MRCID_Code AS code_cim,
    MRC.MRCID_Desc AS libelle_cim,
    MRC.MRCID_CreatedDate AS cim_created_at,
    MRC.MRCID_UpdatedDate AS cim_updated_at,
    MRC.MRCID_DateActiveFrom AS cim_active_from,
    MRC.MRCID_DateActiveTo AS cim_active_to,

    diag.PROB_EntryStatus AS diagnostic_status,
    diag.PROB_Deleted AS diagnostic_deleted_flag,
    diag.PROB_CreateDate AS date_diagnostic_created_at,
    diag.PROB_UpdateDate AS date_diagnostic_updated_at,
    diag.PROB_EndDate AS date_diagnostic_end

FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_Person pe ON pe.PAPER_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.PA_Problem diag ON diag.PROB_ParRef = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MRC_ICDDx MRC ON diag.PROB_ICDCode_DR = MRC.MRCID_RowId

WHERE pat.PAPMI_No IN ('199905722', '201501921', '199203430', '201000423', '202202926', '202111877', '198403893', '201903845', '201707158', '201306063', '201703734',
'201511603', '201510367', '201508426', '201508392', '201409384', '201406656', '200202082', '200702825', '198803224', '201331176', '201307100', '201331008', '201326533',
'201321927', '201325387', '201325526', '201330943', '01323926', '201331061', '200003719', '201611692', '202209318', '201901653', '201100518', '201201322', '201409844',
'201330536', '201322683', '201900355', '201508917', '201707604', '201700577', '201000423', '201314903', '201407199', '201608837', '201326179', '201606448', '201706673',
'201201207', '201326476', '201504143', '201405093', '201311949', '201311779', '201401801', '201317407', '201324095', '201603130', '201105562', '201326327', '201403675',
'202209242', '202004418', '201706646', '201504889', '201326177', '201324153', '200904010', '201609568', '200903763', '201326649', '202101618', '202010090')
"""

        cursor.execute(sql_patient_data)
        patient_results = cursor.fetchall()
       
        patient_data = []

        for row in patient_results:
            raw_code_cim = row.code_cim or ""

        # Normalisation + log de traçabilité
            normalized_code_cim = raw_code_cim
            if raw_code_cim and len(raw_code_cim) == 4 and '.' not in raw_code_cim:
                normalized_code_cim = f"{raw_code_cim[:3]}.{raw_code_cim[3:]}"
                logging.warning(f"[NORMALISATION] Code CIM brut transformé → {raw_code_cim} devient {normalized_code_cim}")

            patient_data.append({
                "ipp_ocr": row.ipp_ocr,
                "ipp_chu": row.ipp_chu or "",
                "annee_naissance": int(row.annee_naissance) if row.annee_naissance else None,
                "sexe": row.sexe.encode('utf-8').decode('utf-8') if isinstance(row.sexe, str) else row.sexe,
                "death_of_death": row.death_of_death if isinstance(row.death_of_death, (datetime, date)) else None,

                # Diagnostic (côté patient)
                "condition_start_date": row.date_diagnostic if isinstance(row.date_diagnostic, (datetime, date)) else None,
                "condition_end_date": row.date_diagnostic_end if isinstance(row.date_diagnostic_end, (datetime, date)) else None,
                "condition_create_date": row.date_diagnostic_created_at if isinstance(row.date_diagnostic_created_at, (datetime, date)) else None,
                "condition_update_date": row.date_diagnostic_updated_at if isinstance(row.date_diagnostic_updated_at, (datetime, date)) else None,

                "condition_status": row.diagnostic_status or "",
                "condition_deleted_flag": row.diagnostic_deleted_flag or "",

                "concept_id": normalized_code_cim,
                "condition_source_value": raw_code_cim,
                "condition_concept_label": row.libelle_cim or "",


                "cim_created_at": row.cim_created_at if isinstance(row.cim_created_at, (datetime, date)) else None,
                "cim_updated_at": row.cim_updated_at if isinstance(row.cim_updated_at, (datetime, date)) else None,
                "cim_active_from": row.cim_active_from if isinstance(row.cim_active_from, (datetime, date)) else None,
                "cim_active_to": row.cim_active_to if isinstance(row.cim_active_to, (datetime, date)) else None

            })


        sql_patient_admission = """SELECT
    pat.PAPMI_No AS ipp_ocr,
    adm.PAADM_ADMNo AS visit_episode_id,
    -- Dates et heures d'admission/sortie
    adm.PAADM_AdmDate AS visit_start_date,
    adm.PAADM_AdmTime AS visit_start_time,
    adm.PAADM_DischgDate AS visit_end_date,
    adm.PAADM_DischgTime AS visit_end_time,
    adm.PAADM_EstimDischargeDate AS visit_estimated_end_date,
    adm.PAADM_EstimDischargeTime AS visit_estimated_end_time,
    loc.CTLOC_Desc AS visit_functional_unit,
    -- Autres infos
    adm.PAADM_Type AS visit_type,               -- 'I', 'O', etc.
    adm.PAADM_VisitStatus AS visit_status,
    

    motif.REAENC_Comment AS visit_reason,
    motif.REAENC_CreateDate AS visit_reason_create_date,
    motif.REAENC_Deleted AS visit_reason_deleted_flag,
    adm.PAADM_PreAdmitted AS is_preadmission

FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_ADM adm ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MR_ADM mradm ON mradm.MRADM_ADM_DR = adm.PAADM_RowID
LEFT JOIN SQLUser.MR_ReasonForEnc motif ON motif.REAENC_ParRef = mradm.MRADM_RowId
LEFT JOIN SQLUser.CT_Loc loc ON adm.PAADM_DepCode_DR = loc.CTLOC_RowID  -- ajout de la jointure

WHERE pat.PAPMI_No IN ('199905722', '201501921', '199203430', '201000423', '202202926', '202111877', '198403893', '201903845', '201707158', '201306063', '201703734', 
'201511603', '201510367', '201508426', '201508392', '201409384', '201406656', '200202082', '200702825', '198803224', '201331176', '201307100', '201331008', '201326533',
'201321927', '201325387', '201325526', '201330943', '01323926', '201331061', '200003719', '201611692', '202209318', '201901653', '201100518', '201201322', '201409844',
'201330536', '201322683', '201900355', '201508917', '201707604', '201700577', '201000423', '201314903', '201407199', '201608837', '201326179', '201606448', '201706673',
'201201207', '201326476', '201504143', '201405093', '201311949', '201311779', '201401801', '201317407', '201324095', '201603130', '201105562', '201326327', '201403675',
'202209242', '202004418', '201706646', '201504889', '201326177', '201324153', '200904010', '201609568', '200903763', '201326649', '202101618', '202010090')

"""

        cursor.execute(sql_patient_admission)
        admission_results = cursor.fetchall()

        admission_data = []
        for row in admission_results:

            preadmission_bool = True if (row.visit_status or "").strip().upper() == "P" else False

            admission_data.append({
                "ipp_ocr": row.ipp_ocr,
                "visit_episode_id": row.visit_episode_id or "",
                "visit_start_date": row.visit_start_date if isinstance(row.visit_start_date, (datetime, date)) else None,
                "visit_start_time": row.visit_start_time if isinstance(row.visit_start_time, time) else None,
                "visit_end_date": row.visit_end_date if isinstance(row.visit_end_date, (datetime, date)) else None,
                "visit_end_time": row.visit_end_time if isinstance(row.visit_end_time, time) else None,
                "visit_estimated_end_date": row.visit_estimated_end_date if isinstance(row.visit_estimated_end_date, (datetime, date)) else None,
                "visit_estimated_end_time": row.visit_estimated_end_time if isinstance(row.visit_estimated_end_time, time) else None,
                "visit_functional_unit": row.visit_functional_unit or "",
                "visit_type": row.visit_type or "",
                "visit_status": row.visit_status or "",
                "visit_reason": row.visit_reason or "",
                "visit_reason_create_date": row.visit_reason_create_date if isinstance(row.visit_reason_create_date, (datetime, date)) else None,
               #"visit_reason_status": row.visit_reason_status or "",
                "visit_reason_deleted_flag": row.visit_reason_deleted_flag or "",
                "is_preadmission": preadmission_bool
            })

        # Requête pour `patient_weight_history`

        sql_weight_history = """ SELECT
    pat.PAPMI_No AS ipp_ocr,
    MRC.MRCID_Code AS code_cim,
    obs.OBS_Date AS measure_date,
    TO_CHAR(obs.OBS_Time, 'HH24:MI:SS') AS measure_time,
    obs.OBS_UpdateDate AS obs_updated_at,
    itm.ITM_Desc AS measure_type,
    obs.OBS_Value AS measure_value
FROM SQLUser.MR_Observations obs
JOIN SQLUser.MRC_ObservationItem itm ON obs.OBS_Item_DR = itm.ITM_RowId
JOIN SQLUser.MR_ADM madr ON obs.OBS_ParRef = madr.MRADM_RowId
JOIN SQLUser.PA_ADM adm ON madr.MRADM_ADM_DR = adm.PAADM_RowID
JOIN SQLUser.PA_PATMAS pat ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.PA_Problem prob ON prob.PROB_ParRef = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MRC_ICDDx MRC ON prob.PROB_ICDCode_DR = MRC.MRCID_RowId
WHERE pat.PAPMI_No IN (
    '199905722', '201501921', '199203430', '201000423', '202202926', '202111877', '198403893', '201903845',
    '201707158', '201306063', '201703734', '201511603', '201510367', '201508426', '201508392', '201409384',
    '201406656', '200202082', '200702825', '198803224', '201331176', '201307100', '201331008', '201326533',
    '201321927', '201325387', '201325526', '201330943', '01323926', '201331061', '200003719', '201611692',
    '202209318', '201901653', '201100518', '201201322', '201409844', '201330536', '201322683', '201900355',
    '201508917', '201707604', '201700577', '201314903', '201407199', '201608837', '201326179', '201606448',
    '201706673', '201201207', '201326476', '201504143', '201405093', '201311949', '201311779', '201401801',
    '201317407', '201324095', '201603130', '201105562', '201326327', '201403675', '202209242', '202004418',
    '201706646', '201504889', '201326177', '201324153', '200904010', '201609568', '200903763', '201326649',
    '202101618', '202010090'
)
AND (
    itm.ITM_Desc IN (
        'POIDS', 'TAILLE', 'INDICEDEMASSECORPORELLE', -- poids, taille, imc
        'Score OMS / Karnofsky', 'KARNOFSKY', 'OMS'   -- scores (si plusieurs noms possibles)
    )
)
AND obs.OBS_Time > 0
ORDER BY pat.PAPMI_No, obs.OBS_Date ASC, itm.ITM_Desc
"""


        cursor.execute(sql_weight_history)
        weight_results = cursor.fetchall()

        measure_data = []
        for row in weight_results:
            try:
                raw = {
                    "ipp_ocr": row.ipp_ocr,
                    "measure_date": row.measure_date if isinstance(row.measure_date, (datetime, date)) else None,
                    "measure_time": datetime.strptime(row.measure_time, "%H:%M:%S").time() if row.measure_time else None,
                    "obs_update_at": row.obs_updated_at if isinstance(row.obs_updated_at, (datetime, date)) else None,
                    "code_cim": row.code_cim,
                    "measure_type": row.measure_type,
                    "measure_value": str(row.measure_value) if row.measure_value is not None else "",
                }
                measure_data.append(raw)

            except Exception as row_error:
                logging.error(f"Erreur ligne measure : {row} → {row_error}")
                continue

        # Retour JSON-serializable pour Airflow
        return (
            make_serializable(patient_data),
            make_serializable(admission_data),
            make_serializable(measure_data)
        )

    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données : {e}")
        raise


def compute_visit_hash(row):
    key_parts = [
        str(row.get("patient_id") or ""),
        str(row.get("visit_episode_id") or ""),
        str(row.get("visit_start_date") or ""),
        str(row.get("visit_start_time") or ""),
        str(row.get("visit_end_date") or ""),
        str(row.get("visit_end_time") or ""),
        str(row.get("visit_estimated_end_date") or ""),
        str(row.get("visit_estimated_end_time") or ""),
        str(row.get("visit_functional_unit") or ""),
        str(row.get("visit_type") or ""),
        str(row.get("visit_status") or ""),
        str(row.get("visit_reason") or ""),
        str(row.get("visit_reason_create_date") or ""),
       #str(row.get("visit_reason_status") or ""),
        str(row.get("visit_reason_deleted_flag") or ""),
        str(row.get("is_preadmission") or ""),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


def compute_condition_hash(row):
    key_parts = [
        str(row.get("patient_id") or ""),
        str(row.get("concept_id") or ""),
        str(row.get("condition_source_value") or ""),
        str(row.get("condition_concept_label") or ""),
        str(row.get("libelle_cim_reference") or ""),
        str(row.get("condition_start_date") or ""),
        str(row.get("condition_end_date") or ""),
        str(row.get("condition_status") or ""),
        str(row.get("condition_deleted_flag") or ""),
        str(row.get("condition_create_date") or ""),
        str(row.get("condition_update_date") or ""),
        str(row.get("cim_created_at") or ""),
        str(row.get("cim_updated_at") or ""),
        str(row.get("cim_active_from") or ""),
        str(row.get("cim_active_to") or ""),
    ]
    concatenated = "|".join(key_parts)
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest()


# ETL - Chargement dans Postgrte
def load_to_postgresql(**kwargs):
    logging.info("Début du chargement dans PostgreSQL")

    credentials = load_credentials("credentials.yml")
    postgres_config = credentials['database']['postgresql']

    # Connexion Postgres
    conn = psycopg2.connect(
        host=postgres_config['host'],
        port=postgres_config['port'],
        user=postgres_config['user'],
        password=postgres_config['password'],
        database=postgres_config['database']
    )
    cur = conn.cursor()

    # Vérifie où PostgreSQL va chercher par défaut
    cur.execute("SHOW search_path")
    search_path = cur.fetchone()
    logging.info("Search path actuel : %s", search_path)

    # Récupération du référentiel CIM10
    cur.execute("SELECT code, description FROM osiris.cim10")
    rows = cur.fetchall()
    logging.info("nb lignes récupérés dans cim10 : %s", len(rows))
    if rows:
        logging.info("Exemple d'une ligne : %s", rows[0])
    else:
        logging.warning("Aucune ligne trouvée dans dbo.cim10")

    cim_reference_map = {row[0]: row[1] for row in rows}


    ti = kwargs['ti']
    patient_data, admission_data, measure_data = ti.xcom_pull(task_ids='extract_data_from_iris_osiris')

    # Pour charger dans osiris.person (uniquement une ligne par ipp_ocr)
    unique_patients = {}
    for d in patient_data:
        key = d.get("ipp_ocr")
        if key and key not in unique_patients:
            unique_patients[key] = (
                d.get("ipp_ocr"),
                d.get("ipp_chu"),
                d.get("annee_naissance"),
                d.get("sexe"),
                d.get("death_of_death")
            )

    rows_to_insert_patient = list(unique_patients.values())

    if rows_to_insert_patient:
        execute_values(cur, """
            INSERT INTO osiris.patient (
                ipp_ocr, ipp_chu, year_of_birth, gender, date_of_death
            ) VALUES %s
            ON CONFLICT ON CONSTRAINT patient_ipp_ocr_unique DO NOTHING
        """, rows_to_insert_patient)
        conn.commit()
        logging.info(f"{len(rows_to_insert_patient)} lignes insérées dans osiris.patient.")
    else:
        logging.info("Aucune nouvelle donnée à insérer dans osiris.patient (tout est déjà présent).")

    # Rafraîchir le mapping ID
    cur.execute("SELECT patient_id, ipp_ocr FROM osiris.patient")
    ipp_to_id = {row[1]: row[0] for row in cur.fetchall()}


    # Charger dans osiris.condition_occurrence avec déduplication
    # Générer les lignes à insérer dans condition_occurrence
    rows_to_insert_conditions = []

    for d in patient_data:
        patient_id = ipp_to_id.get(d["ipp_ocr"])
        if not patient_id:
            continue

        libelle_cim_reference = cim_reference_map.get(d.get("condition_source_value"))

        row_dict = {
            "patient_id": patient_id,
            "concept_id": d.get("concept_id"),
            "condition_source_value": d.get("condition_source_value"),
            "condition_concept_label": d.get("condition_concept_label"),
            "libelle_cim_reference": libelle_cim_reference,
            "condition_start_date": d.get("condition_start_date"),
            "condition_end_date": d.get("condition_end_date"),
            "condition_status": d.get("condition_status"),
            "condition_deleted_flag": d.get("condition_deleted_flag"),
            "condition_create_date": d.get("condition_create_date"),
            "condition_update_date": d.get("condition_update_date"),
            "cim_created_at": d.get("cim_created_at"),
            "cim_updated_at": d.get("cim_updated_at"),
            "cim_active_from": d.get("cim_active_from"),
            "cim_active_to": d.get("cim_active_to"),
        }

        row_hash = compute_condition_hash(row_dict)

        rows_to_insert_conditions.append(tuple(row_dict.values()) + (row_hash,))

    if rows_to_insert_conditions:
        execute_values(cur, """
            INSERT INTO osiris.condition (
                patient_id,
                concept_id,
                condition_source_value,
                condition_concept_label,
                libelle_cim_reference,
                condition_start_date,
                condition_end_date,
                condition_status,
                condition_deleted_flag,
                condition_create_date,
                condition_update_date,
                cim_created_at,
                cim_updated_at,
                cim_active_from,
                cim_active_to,
                condition_hash
            ) VALUES %s
            ON CONFLICT (condition_hash) DO NOTHING
        """, rows_to_insert_conditions)

        conn.commit()
        logging.info(f"{len(rows_to_insert_conditions)} lignes proposées à l'insertion dans osiris.condition (certaines peuvent déjà exister).")
    else:
        logging.info("Aucune nouvelle donnée à insérer dans osiris.condition_occurence (tout est déjà présent).")

    # Charger dans osiris.visit_occurrence (avec dédoublonnage)
    rows_to_insert_visit=[]
    for v in admission_data:
        patient_id = ipp_to_id.get(v["ipp_ocr"])
        if not patient_id:
            continue

    # Clé d'unicité basée sur tous les champs de la visite
        row = {
            "patient_id": patient_id,
            "visit_episode_id": v.get("visit_episode_id"),
            "visit_start_date": v.get("visit_start_date"),
            "visit_start_time": v.get("visit_start_time"),
            "visit_end_date": v.get("visit_end_date"),
            "visit_end_time": v.get("visit_end_time"),
            "visit_estimated_end_date": v.get("visit_estimated_end_date"),
            "visit_estimated_end_time": v.get("visit_estimated_end_time"),
            "visit_functional_unit": v.get("visit_functional_unit"),
            "visit_type": v.get("visit_type"),
            "visit_status": v.get("visit_status"),
            "visit_reason": v.get("visit_reason"),
            "visit_reason_create_date": v.get("visit_reason_create_date"),
           #"visit_reason_status": v.get("visit_reason_status"),
            "visit_reason_deleted_flag": v.get("visit_reason_deleted_flag"),
            "is_preadmission": v.get("is_preadmission"),
        }
        visit_hash = compute_visit_hash(row)
        rows_to_insert_visit.append(tuple(row.values()) + (visit_hash,))

    if rows_to_insert_visit:
        execute_values(cur, """
            INSERT INTO osiris.visit_occurrence (
                patient_id,
                visit_episode_id,
                visit_start_date,
                visit_start_time,
                visit_end_date,
                visit_end_time,
                visit_estimated_end_date,
                visit_estimated_end_time,
                visit_functional_unit,
                visit_type,
                visit_status,
                visit_reason,
                visit_reason_create_date,
                visit_reason_deleted_flag,
                is_preadmission,
                visit_hash
            ) VALUES %s
            ON CONFLICT (visit_hash) DO NOTHING
        """, rows_to_insert_visit)
        conn.commit()
        logging.info(f"{len(rows_to_insert_visit)} lignes insérées dans osiris.visit_occurence.")
    else:
        logging.info("Aucune nouvelle donnée à insérer dans osiris.visit_occurence (tout est déjà présent).")

    # Charger dans osiris.raw_measurement avec déduplication côté Python
    rows_to_insert_measure = []
    seen_measure_hashes = set()

    for m in measure_data:
        patient_id = ipp_to_id.get(m["ipp_ocr"])
        if not patient_id:
            continue

        row = {
            "patient_id": patient_id,
            "measure_date": m.get("measure_date"),
            "measure_time": m.get("measure_time"),
            "obs_update_at": m.get("obs_update_at"),
            "code_cim": m.get("code_cim"),
            "measure_type": m.get("measure_type"),
            "measure_value": m.get("measure_value"),
        }

        # Générer le hash de la ligne
        hash_str = hashlib.sha256("|".join([str(v or "") for v in row.values()]).encode("utf-8")).hexdigest()
        row["measure_hash"] = hash_str

        # Ne pas insérer si le hash est déjà vu
        if hash_str not in seen_measure_hashes:
            seen_measure_hashes.add(hash_str)
            rows_to_insert_measure.append(tuple(row.values()))

    # Insertion finale
    if rows_to_insert_measure:
        execute_values(cur, """
            INSERT INTO osiris.measure (
                patient_id, measure_date, measure_time, obs_update_at, code_cim,
                measure_type, measure_value, measure_hash
            ) VALUES %s
            ON CONFLICT (measure_hash) DO NOTHING
        """, rows_to_insert_measure)
        conn.commit()
        logging.info(f"{len(rows_to_insert_measure)} lignes insérées dans osiris.measure (après déduplication).")
    else:
        logging.info("Aucune nouvelle donnée à insérer dans osiris.raw_measurement (tout est déjà présent).")
    
    for m in measure_data:
        m["patient_id"] = ipp_to_id.get(m["ipp_ocr"])

    enriched_rows = enrich_measure_data(measure_data)
    if enriched_rows:
        execute_values(cur, """
            INSERT INTO osiris.measure_enriched (
                patient_id,
                measure_date,
                measure_time,
                obs_update_at,
                code_cim,
                poids,
                taille,
                imc,
                imc_calcule,
                score_oms,
                score_karnofsky,
                first_line_oms_score,
                enrished_hash
            ) VALUES %s
            ON CONFLICT (enrished_hash) DO NOTHING
        """, enriched_rows)
        conn.commit()
        logging.info(f"{len(enriched_rows)} lignes insérées dans osiris.measure_enriched.")
    else:
        logging.info("Aucune nouvelle 	donnée à insérer dans osiris.measure_enriched.")

    logging.info("Chargement terminé avec ou sans insertion de nouvelles lignes.")
    cur.close()
    conn.close()


# **Définition du DAG Airflow**
default_args = {
    'owner': 'DATA-IA',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_osiris_postgreSQL',
    default_args=default_args,
    description='Extraction de IRIS ODBC et chargement dans PostgreSQL',
    schedule_interval=None,
    catchup=False
)

#**Tâches Airflow**
extract_task_iris = PythonOperator(
    task_id='extract_data_from_iris_osiris',
    python_callable=extract_data_from_iris,
    provide_context=True,
    dag=dag,
    do_xcom_push=True
)

load_task = PythonOperator(
    task_id='load_to_postgresql_osiris',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag
)

# **Ordre d'exécution**
extract_task_iris >> load_task
