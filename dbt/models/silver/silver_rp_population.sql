{{
  config(
    materialized='table',
    tags=['silver', 'social', 'rp'],
    description='RP 2022 – Structure démographique et CSP par IRIS',
    meta={'source_ids': ['S02'], 'millesime': '2022', 'pk': 'iris_code'}
  )
}}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'raw_rp_population_iris') }}
),

cleaned AS (
    SELECT
        {{ normalize_iris_code('iris') }}   AS iris_code,

        -- Pop totale
        {{ safe_numeric('p22_pop', 18, 6) }} AS population_totale,

        -- Structure par age
        {{ safe_numeric('p22_pop0002', 18, 6) }} AS pop_0_2_ans,
        {{ safe_numeric('p22_pop0305', 18, 6) }} AS pop_3_5_ans,
        {{ safe_numeric('p22_pop0610', 18, 6) }} AS pop_6_10_ans,
        {{ safe_numeric('p22_pop1117', 18, 6) }} AS pop_11_17_ans,
        {{ safe_numeric('p22_pop1824', 18, 6) }} AS pop_18_24_ans,
        {{ safe_numeric('p22_pop2539', 18, 6) }} AS pop_25_39_ans,
        {{ safe_numeric('p22_pop4054', 18, 6) }} AS pop_40_54_ans,
        {{ safe_numeric('p22_pop5564', 18, 6) }} AS pop_55_64_ans,
        {{ safe_numeric('p22_pop6579', 18, 6) }} AS pop_65_79_ans,
        {{ safe_numeric('p22_pop80p', 18, 6) }}  AS pop_80_ans_plus,

        -- CSP 15 ans et plus
        {{ safe_numeric('c22_pop15p_stat_gsec11_21', 18, 6) }} AS nb_agriculteurs,
        {{ safe_numeric('c22_pop15p_stat_gsec12_22', 18, 6) }} AS nb_artisans_commercants,
        {{ safe_numeric('c22_pop15p_stat_gsec13_23', 18, 6) }} AS nb_cadres_prof_intel,
        {{ safe_numeric('c22_pop15p_stat_gsec14_24', 18, 6) }} AS nb_prof_intermediaires,
        {{ safe_numeric('c22_pop15p_stat_gsec15_25', 18, 6) }} AS nb_employes,
        {{ safe_numeric('c22_pop15p_stat_gsec16_26', 18, 6) }} AS nb_ouvriers,
        {{ safe_numeric('c22_pop15p_stat_gsec32', 18, 6) }}    AS nb_retraites,
        {{ safe_numeric('c22_pop15p_stat_gsec40', 18, 6) }}    AS nb_sans_activite,

        '{{ var("millesime_rp_pop") }}'  AS millesime,
        CURRENT_TIMESTAMP                AS dbt_updated_at

    FROM raw
    WHERE iris IS NOT NULL AND TRIM(CAST(iris AS VARCHAR)) != ''
),

-- Calcul des parts
with_rates AS (
    SELECT
        *,
        -- Moins de 25 ans
        CASE WHEN population_totale > 0
            THEN ROUND(
                (COALESCE(pop_0_2_ans,0) + COALESCE(pop_3_5_ans,0) +
                  COALESCE(pop_6_10_ans,0) + COALESCE(pop_11_17_ans,0) +
                  COALESCE(pop_18_24_ans,0))::NUMERIC / population_totale * 100, 2)
            ELSE NULL
        END AS part_moins_25_ans_pct,

        -- 65 ans et plus
        CASE WHEN population_totale > 0
             THEN ROUND(
                 (COALESCE(pop_65_79_ans,0) + COALESCE(pop_80_ans_plus,0))::NUMERIC
                 / population_totale * 100, 2)
             ELSE NULL
        END AS part_plus_65_ans_pct,

        -- cadres parmi les 15+
        CASE WHEN (COALESCE(nb_agriculteurs,0) + COALESCE(nb_artisans_commercants,0) +
                   COALESCE(nb_cadres_prof_intel,0) + COALESCE(nb_prof_intermediaires,0) +
                   COALESCE(nb_employes,0) + COALESCE(nb_ouvriers,0) +
                   COALESCE(nb_retraites,0) + COALESCE(nb_sans_activite,0)) > 0
             THEN ROUND(nb_cadres_prof_intel::NUMERIC /
                  (COALESCE(nb_agriculteurs,0) + COALESCE(nb_artisans_commercants,0) +
                   COALESCE(nb_cadres_prof_intel,0) + COALESCE(nb_prof_intermediaires,0) +
                   COALESCE(nb_employes,0) + COALESCE(nb_ouvriers,0) +
                   COALESCE(nb_retraites,0) + COALESCE(nb_sans_activite,0)) * 100, 2)
             ELSE NULL
        END AS part_cadres_pct

    FROM cleaned
)

SELECT wr.*
FROM with_rates wr
INNER JOIN {{ ref('silver_referentiel_iris') }} r
    ON wr.iris_code = r.iris_code
