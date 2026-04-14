{{
  config(
    materialized='table',
    tags=['gold', 'esg', 'social'],
    description='''
    Pilier S – Indicateurs Sociaux par IRIS.
    Sources : FILOSOFI, RP Population, RP Activité.
    Score S : normalisé 0–100 (100 = situation sociale la plus favorable).
    Composantes :
      - 35% : Taux de chômage inversé
      - 35% : Revenu médian normalisé
      - 30% : Taux de pauvreté inversé
    IMPORTANT : SCORE INDICATIF.
    ''',
    meta={
      'owner': 'data-engineering',
      'source_ids': ['S01', 'S02', 'S03'],
      'pk': 'iris_code',
    }
  )
}}

WITH

ref_iris    AS (SELECT * FROM {{ ref('silver_referentiel_iris') }}),
filosofi    AS (SELECT * FROM {{ ref('silver_filosofi') }}),
rp_pop      AS (SELECT * FROM {{ ref('silver_rp_population') }}),
rp_activite AS (SELECT * FROM {{ ref('silver_rp_activite') }})

SELECT
    r.iris_code,
    r.iris_label,
    r.bv_code,
    r.dep_code,

    -- Population
    p.population_totale,
    p.part_moins_25_ans_pct,
    p.part_plus_65_ans_pct,
    p.part_cadres_pct,
    p.nb_cadres_prof_intel,
    p.nb_ouvriers,
    p.nb_employes,

    -- Revenus et pauvreté (FILOSOFI)
    f.revenu_median_uc,
    f.revenu_q1_uc,
    f.revenu_q3_uc,
    f.revenu_d1_uc,
    f.revenu_d9_uc,
    f.taux_pauvrete_60,
    f.taux_pauvrete_40,
    f.part_menages_imposes,
    f.nb_menages_fiscaux,
    f.flag_secret_statistique,

    -- Ratio inter D9/D1 (mesure des inegalites internes à l'IRIS)
    CASE
        WHEN f.revenu_d1_uc > 0
        THEN ROUND(f.revenu_d9_uc / f.revenu_d1_uc, 2)
        ELSE NULL
    END AS ratio_interdecile_d9d1,

    -- Emploi et chômage (RP)
    a.pop_active_1564,
    a.nb_chomeurs_1564,
    a.actifs_occupes_1564,
    a.nb_emplois_aides,
    a.taux_chomage_pct,
    a.taux_emploi_pct,
    a.taux_activite_pct,

    -- Score Social (0–100)
    ROUND(
        -- Chômage inversé (35%) : 0% chômage = 100, 30%+ = 0
        COALESCE(
            GREATEST(0, 100 - COALESCE(a.taux_chomage_pct, 10) * 100.0 / 30),
            50
        ) * 0.35
        +
        -- Revenu médian normalisé (35%) : 30k€/UC = 100, 10k€ = 0 (seuil arbitraire)
        COALESCE(
            LEAST(100, GREATEST(0, (COALESCE(f.revenu_median_uc, 20000) - 10000) / 200.0)),
            50
        ) * 0.35
        +
        -- Pauvreté inversée (30%) : 0% = 100, 50%+ = 0
        COALESCE(
            GREATEST(0, 100 - COALESCE(f.taux_pauvrete_60, 15) * 2),
            50
        ) * 0.30,
        2
    ) AS score_social_0_100,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM ref_iris r
LEFT JOIN filosofi    f ON r.iris_code = f.iris_code
LEFT JOIN rp_pop      p ON r.iris_code = p.iris_code
LEFT JOIN rp_activite a ON r.iris_code = a.iris_code