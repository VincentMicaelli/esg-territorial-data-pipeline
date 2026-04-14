{{
  config(
    materialized='table',
    tags=['gold', 'esg', 'environnemental'],
    description='''
    Pilier E – Indicateurs Environnementaux par IRIS.
    Sources : silver.silver_rp_logement (ancienneté parc) et silver.silver_passoires_energetiques.
    Score E : normalisé 0–100 (100 = meilleure performance environnementale).
    Composantes :
      - 40% : Taux de passoires énergétiques inversé (moins = mieux)
      - 35% : Part logements avant 1971 inversée (proxy DPE, moins = mieux)
      - 25% : Taux vacance logements (proxy dégradation urbaine, moins = mieux)
    ''',
    meta={
      'owner': 'data-engineering',
      'source_ids': ['S04', 'S09'],
      'pk': 'iris_code',
    }
  )
}}

WITH

ref_iris AS (SELECT * FROM {{ ref('silver_referentiel_iris') }}),
logement AS (SELECT * FROM {{ ref('silver_rp_logement') }}),
passoires AS (SELECT * FROM {{ ref('silver_passoires_energetiques') }})

SELECT
    r.iris_code,
    r.iris_label,
    r.bv_code,
    r.dep_code,

    -- Parc de logements 
    l.nb_logements_total,
    l.nb_residences_principales,
    l.nb_logements_vacants,
    l.part_proprietaires_pct,
    l.part_hlm_pct,
    l.part_logements_avant_1971_pct,

    -- Taux de vacance (logements vides / parc total)
    CASE
        WHEN l.nb_logements_total > 0
        THEN ROUND(l.nb_logements_vacants::NUMERIC / l.nb_logements_total * 100, 2)
        ELSE NULL
    END AS taux_vacance_logements_pct,

    -- Passoires énergétiques
    p.nb_passoires_energetiques_estime,
    p.taux_passoires_energetiques_pct,
    p.nb_residences_principales_estime,

    -- Score Environnemental (0–100)
    ROUND(
        -- Composante passoires (40%) : 0% = 100, 40%+ = 0
        COALESCE(
            GREATEST(0, 100 - COALESCE(p.taux_passoires_energetiques_pct, 20) * 2.5),
            50
        ) * 0.40
        +
        -- Composante logements anciens (35%) : 0% avant 1971 = 100, 100% = 0
        COALESCE(
            100 - COALESCE(l.part_logements_avant_1971_pct, 50),
            50
        ) * 0.35
        +
        -- Composante vacance logements (25%) : 0% = 100, 20%+ = 0
        COALESCE(
            GREATEST(0, 100 - CASE WHEN l.nb_logements_total > 0
                THEN l.nb_logements_vacants::NUMERIC / l.nb_logements_total * 500
                ELSE 50 END),
            50
        ) * 0.25,
        2
    ) AS score_env_0_100,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM ref_iris r
LEFT JOIN logement   l ON r.iris_code = l.iris_code
LEFT JOIN passoires  p ON r.iris_code = p.iris_code
