{{
  config(
    materialized='table',
    tags=['silver', 'env', 'rp'],
    description='RP 2022 – Parc de logements et statut d''occupation par IRIS',
    meta={'source_ids': ['S04'], 'millesime': '2022', 'pk': 'iris_code'}
  )
}}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'raw_rp_logement_iris') }}
),

cleaned AS (
    SELECT
        {{ normalize_iris_code('iris') }}   AS iris_code,

        -- Parc de logements
        {{ safe_numeric('p22_log', 18, 6) }}     AS nb_logements_total,
        {{ safe_numeric('p22_rp', 18, 6) }}      AS nb_residences_principales,
        {{ safe_numeric('p22_rsecocc', 18, 6) }} AS nb_residences_secondaires,
        {{ safe_numeric('p22_logvac', 18, 6) }}  AS nb_logements_vacants,

        -- Statut d'occupation
        {{ safe_numeric('p22_rp_prop', 18, 6) }}    AS nb_proprietaires,
        {{ safe_numeric('p22_rp_loc', 18, 6) }}     AS nb_locataires,
        {{ safe_numeric('p22_rp_lochlmv', 18, 6) }} AS nb_locataires_hlm,
        {{ safe_numeric('p22_rp_grat', 18, 6) }}    AS nb_logts_gratuits,

        -- Anciennete de construction
        {{ safe_numeric('p22_rp_ach1919', 18, 6) }} AS nb_logts_avant_1919,
        {{ safe_numeric('p22_rp_ach1945', 18, 6) }} AS nb_logts_1919_1945,
        {{ safe_numeric('p22_rp_ach1970', 18, 6) }} AS nb_logts_1946_1970,
        {{ safe_numeric('p22_rp_ach1990', 18, 6) }} AS nb_logts_1971_1990,
        {{ safe_numeric('p22_rp_ach2005', 18, 6) }} AS nb_logts_1991_2005,
        {{ safe_numeric('p22_rp_ach2019', 18, 6) }} AS nb_logts_2006_2019,

        -- Taille des logements
        {{ safe_numeric('p22_rp_1p', 18, 6) }}  AS nb_logts_1_piece,
        {{ safe_numeric('p22_rp_2p', 18, 6) }}  AS nb_logts_2_pieces,
        {{ safe_numeric('p22_rp_3p', 18, 6) }}  AS nb_logts_3_pieces,
        {{ safe_numeric('p22_rp_4p', 18, 6) }}  AS nb_logts_4_pieces,
        {{ safe_numeric('p22_rp_5pp', 18, 6) }} AS nb_logts_5_pieces_plus,

        -- Parts calculees
        CASE
            WHEN {{ safe_numeric('p22_rp', 18, 6) }} > 0
            THEN ROUND(
                {{ safe_numeric('p22_rp_prop', 18, 6) }} /
                {{ safe_numeric('p22_rp', 18, 6) }} * 100, 2)
            ELSE NULL
        END AS part_proprietaires_pct,

        CASE
            WHEN {{ safe_numeric('p22_rp', 18, 6) }} > 0
            THEN ROUND(
                {{ safe_numeric('p22_rp_lochlmv', 18, 6) }} /
                {{ safe_numeric('p22_rp', 18, 6) }} * 100, 2)
            ELSE NULL
        END AS part_hlm_pct,

        -- Logements construits avant 1971 (proxy DPE energies)
        CASE
            WHEN {{ safe_numeric('p22_rp', 18, 6) }} > 0
            THEN ROUND(
                (COALESCE({{ safe_numeric('p22_rp_ach1919', 18, 6) }}, 0) +
                 COALESCE({{ safe_numeric('p22_rp_ach1945', 18, 6) }}, 0) +
                 COALESCE({{ safe_numeric('p22_rp_ach1970', 18, 6) }}, 0))
                / {{ safe_numeric('p22_rp', 18, 6) }} * 100, 2)
            ELSE NULL
        END AS part_logements_avant_1971_pct,

        '{{ var("millesime_rp_log") }}' AS millesime,
        CURRENT_TIMESTAMP               AS dbt_updated_at

    FROM raw
    WHERE iris IS NOT NULL AND TRIM(CAST(iris AS VARCHAR)) != ''
)

SELECT c.*
FROM cleaned c
INNER JOIN {{ ref('silver_referentiel_iris') }} r ON c.iris_code = r.iris_code
