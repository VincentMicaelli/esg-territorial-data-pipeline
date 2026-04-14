-- Agrégation des indicateurs ESG à la maille Bassin de Vie.
-- Utile pour les analyses macro-territoriales.
{{
  config(
    materialized='table',
    tags=['gold', 'esg', 'bassin_vie'],
    description='''
    Indicateurs ESG agrégés à la maille Bassin de Vie.
    Méthode : moyenne pondérée par la population (indicateurs de flux)
    ou somme (indicateurs de stock comme population, équipements).
    Un BV = une ligne.
    ''',
    meta={'pk': 'bv_code'}
  )
}}

WITH

iris_data AS (
    SELECT * FROM {{ ref('gold_esg_iris_final') }}
    WHERE population_totale IS NOT NULL
      AND population_totale > 0
)

SELECT
    bv_code,
    bv_label,
    dep_code,

    -- Population
    SUM(population_totale)                              AS population_totale_bv,
    COUNT(iris_code)                                    AS nb_iris,

    -- Indicateurs S ponderes par population
    ROUND(
        SUM(COALESCE(revenu_median_uc, 0) * population_totale) /
        NULLIF(SUM(CASE WHEN revenu_median_uc IS NOT NULL THEN population_totale ELSE 0 END), 0),
        2
    )                                                   AS revenu_median_uc_pondere,

    ROUND(
        SUM(COALESCE(taux_pauvrete_60, 0) * population_totale) /
        NULLIF(SUM(CASE WHEN taux_pauvrete_60 IS NOT NULL THEN population_totale ELSE 0 END), 0),
        2
    )                                                   AS taux_pauvrete_60_pondere,

    ROUND(
        SUM(COALESCE(taux_chomage_pct, 0) * COALESCE(pop_active_1564, 0)) /
        NULLIF(SUM(CASE WHEN taux_chomage_pct IS NOT NULL THEN COALESCE(pop_active_1564, 0) ELSE 0 END), 0),
        2
    )                                                   AS taux_chomage_pondere,

    ROUND(AVG(ratio_interdecile_d9d1), 2)               AS ratio_interdecile_moy,

    -- Indicateurs E ponderes
    ROUND(
        SUM(COALESCE(taux_passoires_energetiques_pct, 0) * COALESCE(nb_residences_principales, 0)) /
        NULLIF(SUM(CASE
            WHEN taux_passoires_energetiques_pct IS NOT NULL
            THEN COALESCE(nb_residences_principales, 0)
            ELSE 0
        END), 0),
        2
    )                                                   AS taux_passoires_energetiques_pondere,

    ROUND(SUM(COALESCE(nb_passoires_energetiques_estime, 0)), 2) AS nb_passoires_energetiques_estime_bv,

    ROUND(AVG(part_logements_avant_1971_pct), 2)        AS part_logts_avant_1971_moy,

    -- Indicateurs G cumules 
    SUM(nb_equipements_total)                           AS nb_equipements_total_bv,
    SUM(nb_equip_sante)                                 AS nb_equip_sante_bv,
    SUM(nb_medecins_generalistes)                       AS nb_medecins_bv,
    ROUND(AVG(indice_presence_services), 2)             AS indice_services_moy,
    COUNT(*) FILTER (WHERE est_zrr = TRUE)              AS nb_iris_zrr,

    -- Scores ESG aggr
    ROUND(
        SUM(COALESCE(score_social_0_100, 50) * population_totale) / SUM(population_totale),
        2
    )                                                   AS score_social_pondere,
    ROUND(
        SUM(COALESCE(score_env_0_100, 50) * population_totale) / SUM(population_totale),
        2
    )                                                   AS score_env_pondere,
    ROUND(
        SUM(COALESCE(score_gouvernance_0_100, 50) * population_totale) / SUM(population_totale),
        2
    )                                                   AS score_gouvernance_pondere,
    ROUND(
        SUM(COALESCE(score_esg_0_100, 50) * population_totale) / SUM(population_totale),
        2
    )                                                   AS score_esg_pondere,

    -- Completude
    ROUND(AVG(pct_completude), 1)                       AS completude_moy_pct,
    COUNT(*) FILTER (WHERE pct_completude < 40)         AS nb_iris_incomplets,

    CURRENT_TIMESTAMP                                   AS dbt_updated_at,
    '{{ var("version_modele") }}'                       AS version_modele

FROM iris_data
GROUP BY bv_code, bv_label, dep_code
