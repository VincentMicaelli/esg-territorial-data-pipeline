{{
  config(
    materialized='table',
    tags=['silver', 'social', 'filosofi'],
    description="FILOSOFI 2021 nettoye a la maille IRIS, filtre sur le referentiel interne.",
    meta={
      'owner': 'data-engineering',
      'source_ids': ['S01'],
      'millesime': '2021',
      'pk': 'iris_code',
    }
  )
}}

WITH

raw as (
    SELECT * FROM {{ source('bronze', 'raw_filosofi_iris') }}
),

cleaned AS (
    SELECT
        {{ normalize_iris_code('iris') }}   AS iris_code,

        -- Revenus dispo par unite de conso
        {{ safe_numeric('disp_med21') }}    AS revenu_median_uc,
        {{ safe_numeric('disp_q121') }}     AS revenu_q1_uc,
        {{ safe_numeric('disp_q321') }}     AS revenu_q3_uc,
        {{ safe_numeric('disp_d121') }}     AS revenu_d1_uc,
        {{ safe_numeric('disp_d921') }}     AS revenu_d9_uc,

        -- Indicateurs de pauvrete et inegalites
        {{ safe_numeric('disp_tp6021', 5, 2) }} AS taux_pauvrete_60,
        NULL::NUMERIC(5, 2)                    AS taux_pauvrete_40,
        NULL::NUMERIC(5, 2)                    AS taux_pauvrete_50,

        -- Menages impose
        {{ safe_numeric('disp_pimpot21', 5, 2) }} AS part_menages_imposes,
        NULL::INTEGER                            AS nb_menages_fiscaux,

        -- Flag secret stat ( INSEE masque si < 11 menages)
        CASE
            WHEN UPPER(TRIM(CAST(disp_med21 AS VARCHAR))) IN ('S', 'ND')
                AND UPPER(TRIM(CAST(disp_tp6021 AS VARCHAR))) IN ('S', 'ND')
            THEN TRUE
            ELSE FALSE
        END                                 AS flag_secret_statistique,

        '{{ var("millesime_filosofi") }}'   AS millesime,
        CURRENT_TIMESTAMP                   AS dbt_updated_at

    FROM raw
    WHERE iris IS NOT NULL
        AND TRIM(CAST(iris AS VARCHAR)) != ''
),

-- Jointure avec le referentiel interne
joined AS (
    SELECT c.*
    FROM cleaned c
    INNER JOIN {{ ref('silver_referentiel_iris') }} r
        ON c.iris_code = r.iris_code
)

SELECT * FROM joined
