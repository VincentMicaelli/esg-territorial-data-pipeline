{{
  config(
    materialized='table',
    tags=['silver', 'social', 'rp'],
    description='RP 2022 – Activité, emploi et chômage par IRIS',
    meta={'source_ids': ['S03'], 'millesime': '2022', 'pk': 'iris_code'}
  )
}}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'raw_rp_activite_iris') }}
),

cleaned AS (
    SELECT
        {{ normalize_iris_code('iris') }}    AS iris_code,

        -- Population active et emploi
        {{ safe_numeric('p22_pop1564', 18, 6) }}    AS pop_1564,
        {{ safe_numeric('p22_act1564', 18, 6) }}    AS pop_active_1564,
        {{ safe_numeric('p22_actocc1564', 18, 6) }} AS actifs_occupes_1564,
        {{ safe_numeric('p22_chom1564', 18, 6) }}   AS nb_chomeurs_1564,
        {{ safe_numeric('p22_inact1564', 18, 6) }}  AS nb_inactifs_1564,

        -- Emplois aides et non-salaries
        {{ safe_numeric('p22_sal15p_empaid', 18, 6) }} AS nb_emplois_aides,
        {{ safe_numeric('p22_nsal15p', 18, 6) }}       AS nb_non_salaries,

        -- Taux de chomage BIT (actifs cherchant emploi / actifs totaux)
        CASE
            WHEN {{ safe_numeric('p22_act1564', 18, 6) }} > 0
            THEN ROUND(
                {{ safe_numeric('p22_chom1564', 18, 6) }} /
                {{ safe_numeric('p22_act1564', 18, 6) }} * 100, 2)
            ELSE NULL
        END AS taux_chomage_pct,

        -- Taux d'emploi (actifs ccupes / pop 15-64 ans)
        CASE
            WHEN {{ safe_numeric('p22_pop1564', 18, 6) }} > 0
            THEN ROUND(
                {{ safe_numeric('p22_actocc1564', 18, 6) }} /
                {{ safe_numeric('p22_pop1564', 18, 6) }} * 100, 2)
            ELSE NULL
        END AS taux_emploi_pct,

        -- Taux d'activite (actifs / pop 15-64 ans)
        CASE
            WHEN {{ safe_numeric('p22_pop1564', 18, 6) }} > 0
            THEN ROUND(
                {{ safe_numeric('p22_act1564', 18, 6) }} /
                {{ safe_numeric('p22_pop1564', 18, 6) }} * 100, 2)
            ELSE NULL
        END AS taux_activite_pct,

        '{{ var("millesime_rp_act") }}' AS millesime,
        CURRENT_TIMESTAMP               AS dbt_updated_at

    FROM raw
    WHERE iris IS NOT NULL AND TRIM(CAST(iris AS VARCHAR)) != ''
)

SELECT c.*
FROM cleaned c
INNER JOIN {{ ref('silver_referentiel_iris') }} r ON c.iris_code = r.iris_code
