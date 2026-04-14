{{
  config(
    materialized='table',
    tags=['silver', 'gouvernance', 'bpe'],
    description='''
    BPE 2024 – Dénombrement des équipements agrégés par IRIS et domaine.
    La source 2024 est déjà pré-agrégée par type et par IRIS ; ce modèle
    somme OBS_VALUE pour reconstruire les totaux thématiques.
    Domaines : A=santé, B=enseignement, C=transports, D=sports-loisirs,
               E=commerces, F=services, G=tourisme
    ''',
    meta={'source_ids': ['S05'], 'millesime': '2024', 'pk': 'iris_code'}
  )
}}

WITH raw AS (
    -- La BPE 2024 est déjà agrégée : une ligne par type d'équipement et par IRIS.
    SELECT
        {{ normalize_iris_code('geo') }} AS iris_code,
        UPPER(TRIM(CAST(facility_type AS VARCHAR))) AS type_equip,
        UPPER(TRIM(CAST(facility_dom AS VARCHAR))) AS domaine,
        COALESCE(CAST(NULLIF(TRIM(CAST(obs_value AS VARCHAR)), '') AS INTEGER), 0) AS nb_equipements
    FROM {{ source('bronze', 'raw_bpe_iris') }}
    WHERE geo IS NOT NULL
      AND TRIM(CAST(geo AS VARCHAR)) != ''
),

-- Aggr par IRIS et domaine
aggregated AS (
    SELECT
        iris_code,

        -- Total equipements
        SUM(nb_equipements)                                     AS nb_equipements_total,

        -- Par domaine
        SUM(nb_equipements) FILTER (WHERE domaine = 'A')        AS nb_equip_sante,
        SUM(nb_equipements) FILTER (WHERE domaine = 'B')        AS nb_equip_enseignement,
        SUM(nb_equipements) FILTER (WHERE domaine = 'C')        AS nb_equip_transports,
        SUM(nb_equipements) FILTER (WHERE domaine = 'D')        AS nb_equip_sports_loisirs,
        SUM(nb_equipements) FILTER (WHERE domaine = 'E')        AS nb_equip_commerces,
        SUM(nb_equipements) FILTER (WHERE domaine = 'F')        AS nb_equip_services,
        SUM(nb_equipements) FILTER (WHERE domaine = 'G')        AS nb_equip_tourisme,

        -- Equipements cles pour acces aux droits (code types INSEE)
        SUM(nb_equipements) FILTER (WHERE type_equip IN ('A206', 'A207', 'A208')) AS nb_medecins_generalistes,
        SUM(nb_equipements) FILTER (WHERE type_equip IN ('A301'))                  AS nb_pharmacies,
        SUM(nb_equipements) FILTER (WHERE type_equip LIKE 'B1%')                   AS nb_ecoles_primaires,
        SUM(nb_equipements) FILTER (WHERE type_equip LIKE 'B2%')                   AS nb_colleges_lycees,
        SUM(nb_equipements) FILTER (WHERE type_equip IN ('C101', 'C102', 'C201'))  AS nb_gares_arrets_tc,
        SUM(nb_equipements) FILTER (WHERE type_equip IN ('E107'))                  AS nb_banques_agences,
        SUM(nb_equipements) FILTER (WHERE type_equip IN ('F116'))                  AS nb_france_services,

        '{{ var("millesime_bpe") }}'  AS millesime,
        CURRENT_TIMESTAMP             AS dbt_updated_at

    FROM raw
    GROUP BY iris_code
)

SELECT a.*
FROM aggregated a
INNER JOIN {{ ref('silver_referentiel_iris') }} r ON a.iris_code = r.iris_code
