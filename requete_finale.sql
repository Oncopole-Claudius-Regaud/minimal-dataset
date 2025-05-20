WITH lignes_raw AS (
  SELECT
    inc.I0CLEUNIK,                       -- identifiant unique de la ligne de traitement
    pa.NOOBSPAT,                         -- identifiant du patient
    inc.COMINCLUS AS treatment_comment,  -- texte libre (souvent observation clinique)
    lt.LIGNETRAIT AS treatment_label,    -- ex: "1ʳᵉ ligne", "2e ligne", "Non applicable"
    lt.LTCLEUNIK,

    prot.NOMPROT AS protocol_name,
    prot.COMMENTPRO AS protocol_detail,
    prot.CATEG AS protocol_category,
    prot.TYPEPROT AS protocol_type,
    prot.CODELOCAL AS local_code,
    prot.VALIDPRO AS valid_protocol,

    pr.NUMCYCLE,
    lp.DATEDEBADMR AS start_date,
    lp.DATEFINADMR AS end_date,
    rp.LIBELLE AS radiation

  FROM CHIMIODATA.INCLUSIO inc
  JOIN CHIMIODATA.PATIENT pa ON pa.PACLEUNIK = inc.PACLEUNIK
  LEFT JOIN CHIMIODATA.LIGNETR lt ON lt.LTCLEUNIK = inc.LTCLEUNIK
  LEFT JOIN CHIMIODATA.PRESCRIP pr ON pr.I0CLEUNIK = inc.I0CLEUNIK
  LEFT JOIN CHIMIODATA.LIGNEPRE lp ON lp.P0CLEUNIK = pr.P0CLEUNIK
    AND lp.ETAT = 8
    AND lp.DCCLEUNIK IS NOT NULL
    AND lp.DCCLEUNIK NOT IN (0, -1)
  LEFT JOIN CHIMIODATA.PROTOCOL prot ON prot.PRCLEUNIK = inc.PRCLEUNIK
  LEFT JOIN CHIMIODATA.RADIOPRO rp ON rp.RACLEUNIK = pr.RACLEUNIK

  WHERE pa.NOOBSPAT IN ('199905722', '201501921', '199203430', '201000423', '202202926', '202111877', '198403893', '201903845', '201707158', '201306063', '201703734',
'201511603', '201510367', '201508426', '201508392', '201409384', '201406656', '200202082', '200702825', '198803224', '201331176', '201307100', '201331008', '201326533',
'201321927', '201325387', '201325526', '201330943', '01323926', '201331061', '200003719', '201611692', '202209318', '201901653', '201100518', '201201322', '201409844',
'201330536', '201322683', '201900355', '201508917', '201707604', '201700577', '201000423', '201314903', '201407199', '201608837', '201326179', '201606448', '201706673',
'201201207', '201326476', '201504143', '201405093', '201311949', '201311779', '201401801', '201317407', '201324095', '201603130', '201105562', '201326327', '201403675',
'202209242', '202004418', '201706646', '201504889', '201326177', '201324153', '200904010', '201609568', '200903763', '201326649', '202101618', '202010090')
),
lignes_grouped AS (
  SELECT
    I0CLEUNIK,
    NOOBSPAT,
    treatment_comment,
    treatment_label,
    protocol_name,
    protocol_detail,
    protocol_category,
    protocol_type,
    local_code,
    valid_protocol,
    MAX(radiation) AS radiation,

    MIN(start_date) AS start_date, -- dates du 1er médoc du cycle
    MAX(end_date) AS end_date,     -- dates du dernier médoc du cycle

    COUNT(DISTINCT NUMCYCLE) AS nb_cycles
  FROM lignes_raw
  GROUP BY
    I0CLEUNIK, NOOBSPAT, treatment_comment, treatment_label,
    protocol_name, protocol_detail, protocol_category,
    protocol_type, local_code, valid_protocol
),
lignes_num AS (
  SELECT
    NOOBSPAT,
    treatment_comment,
    treatment_label,
    protocol_name,
    protocol_detail,
    protocol_category,
    protocol_type,
    local_code,
    valid_protocol,
    start_date,
    end_date,
    nb_cycles,
    radiation,
    ROW_NUMBER() OVER (PARTITION BY NOOBSPAT ORDER BY start_date) AS treatment_line_number
  FROM lignes_grouped
)

SELECT *
FROM lignes_num
ORDER BY NOOBSPAT, treatment_line_number
