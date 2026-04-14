-- ═══════════════════════════════════════════════════════════════════════
-- TABLE MAÎTRE ESG
-- Un IRIS = une ligne. Tous les indicateurs E, S, G consolidés.
-- ═══════════════════════════════════════════════════════════════════════
{{
  config(
    materialized='table',
    tags=['gold', 'esg', 'livrable', 'critique'],
    description='''
    Table finale ESG à la maille IRIS. UN IRIS = UNE LIGNE. Pas de doublons.
    Grain : code_iris (clé primaire).
    Périmètre : IRIS du référentiel interne (S12) uniquement.
    NULL signifie que la donnée source n''est pas disponible pour cet IRIS
    (secret statistique INSEE ou hors champ géographique d''une source).
    pct_completude indique le % d''indicateurs clés renseignés.
    ''',
    meta={
      'owner': 'data-engineering',
      'business_owner': 'Direction Stratégie Territoriale',
      'version': '{{ var("version_modele") }}',
      'row_grain': 'iris_code',
      'pk': 'iris_code',
      'sla': 'Hebdomadaire – disponible chaque lundi matin',
    },
    grants={"select": ["ds_user1", "ds_user2", "da_user"]}
  )
}}

WITH

e   AS (SELECT * FROM {{ ref('gold_indicateurs_e') }}),
s   AS (SELECT * FROM {{ ref('gold_indicateurs_s') }}),
g   AS (SELECT * FROM {{ ref('gold_indicateurs_g') }}),
ref AS (SELECT * FROM {{ ref('silver_referentiel_iris') }})

SELECT

    -- 1. Clés géographiques
    ref.iris_code,
    ref.iris_label,
    ref.bv_code,
    ref.bv_label,
    ref.dep_code,
    ref.com_code,
    ref.is_commune_entiere,

    -- 2. Pilier E : Environnemental
    e.nb_logements_total,
    e.nb_residences_principales,
    e.nb_logements_vacants,
    e.taux_vacance_logements_pct,
    e.part_proprietaires_pct,
    e.part_hlm_pct,
    e.part_logements_avant_1971_pct,
    e.nb_passoires_energetiques_estime,
    e.taux_passoires_energetiques_pct,
    e.nb_residences_principales_estime,
    e.score_env_0_100,

    -- 3. Pilier S : Social
    s.population_totale,
    s.part_moins_25_ans_pct,
    s.part_plus_65_ans_pct,
    s.part_cadres_pct,
    s.revenu_median_uc,
    s.revenu_q1_uc,
    s.revenu_d1_uc,
    s.revenu_d9_uc,
    s.taux_pauvrete_60,
    s.taux_pauvrete_40,
    s.ratio_interdecile_d9d1,
    s.part_menages_imposes,
    s.nb_menages_fiscaux,
    s.taux_chomage_pct,
    s.taux_emploi_pct,
    s.taux_activite_pct,
    s.pop_active_1564,
    s.nb_chomeurs_1564,
    s.nb_emplois_aides,
    s.flag_secret_statistique,
    s.score_social_0_100,

    -- 4. Pilier G : Gouvernance locale
    g.nb_equipements_total,
    g.nb_equip_sante,
    g.nb_equip_enseignement,
    g.nb_equip_transports,
    g.nb_equip_commerces,
    g.nb_equip_services,
    g.nb_medecins_generalistes,
    g.nb_pharmacies,
    g.nb_ecoles_primaires,
    g.nb_colleges_lycees,
    g.nb_gares_arrets_tc,
    g.nb_banques_agences,
    g.nb_france_services,
    g.indice_presence_services,
    g.est_zrr,
    g.score_gouvernance_0_100,

    -- 5. Score ESG Synthétique 
    -- Pondération : S(50%) > E(25%) > G(25%)
    ROUND(
        COALESCE(s.score_social_0_100,     50) * 0.50 +
        COALESCE(e.score_env_0_100,        50) * 0.25 +
        COALESCE(g.score_gouvernance_0_100,50) * 0.25,
        2
    ) AS score_esg_0_100,

    -- 6. Métadonnées qualité
    -- Complétude : % des 10 indicateurs clés renseignés (non NULL)
    ROUND(
        (
            CASE WHEN s.revenu_median_uc           IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.taux_pauvrete_60           IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.taux_chomage_pct           IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.population_totale          IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN e.part_logements_avant_1971_pct IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN e.taux_passoires_energetiques_pct IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN g.nb_equipements_total       IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN g.nb_medecins_generalistes   IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN e.taux_vacance_logements_pct IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN s.ratio_interdecile_d9d1     IS NOT NULL THEN 1 ELSE 0 END
        )::NUMERIC / 10 * 100,
        0
    ) AS pct_completude,

    -- 7. Traçabilité
    CURRENT_TIMESTAMP                       AS dbt_updated_at,
    '{{ var("version_modele") }}'           AS version_modele

FROM ref
LEFT JOIN e ON ref.iris_code = e.iris_code
LEFT JOIN s ON ref.iris_code = s.iris_code
LEFT JOIN g ON ref.iris_code = g.iris_code
