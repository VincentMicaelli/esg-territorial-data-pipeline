{{
  config(
    materialized='table',
    tags=['gold', 'esg', 'gouvernance'],
    description='''
    Pilier G – Indicateurs de Gouvernance locale par IRIS.
    Sources : BPE 2024 (équipements et services), ZRR 2021 (zonages prioritaires).
    Score G : normalisé 0–100 (100 = meilleure accessibilité aux services).
    Composantes :
      - 40% : Indice de présence de services (rang percentile sur le panel banque)
      - 35% : Accès aux services de santé (médecins généralistes)
      - 25% : Accès aux services d''enseignement (écoles + collèges/lycées)
    ''',
    meta={
      'owner': 'data-engineering',
      'source_ids': ['S05', 'S11'],
      'pk': 'iris_code',
    }
  )
}}

WITH

ref_iris AS (
    SELECT * FROM {{ ref('silver_referentiel_iris') }}
),

bpe AS (
    SELECT * FROM {{ ref('silver_bpe') }}
),

-- ── ZRR : Zones de Revitalisation Rurale ─────────────────────────────────────
-- Source S11 : fichier Excel téléchargé depuis data.gouv.fr puis normalisé en Bronze
-- Colonne attendue : CODGEO (code INSEE commune 5 chiffres) + éventuelle colonne de statut
-- Le fichier ZRR liste TOUTES les communes classées ZRR.
-- La logique est : si la commune est présente dans ce fichier → est_zrr = TRUE
-- Si la commune est absente → est_zrr = FALSE (pas ZRR)
zrr_communes AS (
    SELECT DISTINCT
        -- Normaliser le code INSEE sur 5 caractères
        LPAD(
            TRIM(
                CAST(codgeo AS VARCHAR)
            ), 5, '0'
        ) AS com_code,
        TRUE AS est_zrr
    FROM {{ source('bronze', 'raw_zrr_communes') }}
    -- Filtre de sécurité : exclure les lignes sans code commune valide
    WHERE codgeo IS NOT NULL
      AND TRIM(CAST(codgeo AS VARCHAR)) != ''
      -- Le code INSEE commune est toujours numérique (hors Corse : 2A, 2B)
      AND TRIM(CAST(codgeo AS VARCHAR))
          ~ '^(0[1-9]|[1-9][0-9]|2[AB])[0-9]{3}$'
)

SELECT
    r.iris_code,
    r.iris_label,
    r.bv_code,
    r.com_code,
    r.dep_code,

    -- ── Équipements BPE ──────────────────────────────────────────────────────
    b.nb_equipements_total,
    b.nb_equip_sante,
    b.nb_equip_enseignement,
    b.nb_equip_transports,
    b.nb_equip_commerces,
    b.nb_equip_services,
    b.nb_medecins_generalistes,
    b.nb_pharmacies,
    b.nb_ecoles_primaires,
    b.nb_colleges_lycees,
    b.nb_gares_arrets_tc,
    b.nb_banques_agences,
    b.nb_france_services,

    -- ── Indice de présence de services (rang percentile 0–100) ───────────────
    -- PERCENT_RANK() : % d'IRIS ayant MOINS d'équipements que l'IRIS courant
    -- 0 = l'IRIS le moins équipé du panel, 100 = le plus équipé
    ROUND(
        (
            PERCENT_RANK() OVER (ORDER BY COALESCE(b.nb_equipements_total, 0)) * 100
        )::NUMERIC,
        2
    ) AS indice_presence_services,

    -- ── Zonages prioritaires ─────────────────────────────────────────────────
    -- LEFT JOIN sur com_code : les communes non présentes dans raw_zrr_communes → FALSE
    COALESCE(z.est_zrr, FALSE) AS est_zrr,

    -- ── Score Gouvernance (0–100) ────────────────────────────────────────────
    ROUND(
        -- Présence services générale (40%) : rang percentile direct
        (
            PERCENT_RANK() OVER (ORDER BY COALESCE(b.nb_equipements_total, 0)) * 100 * 0.40

            -- Accès santé (35%) : médecins généralistes
            -- Seuil : ≥ 3 médecins dans l'IRIS = score max (100)
            -- 0 médecin dans l'IRIS mais commune non-ZRR = score 30 (accès possible à pied)
            -- 0 médecin + commune ZRR = score 0 (désert médical rural)
            + CASE
                WHEN COALESCE(b.nb_medecins_generalistes, 0) >= 3
                THEN 100.0
                WHEN COALESCE(b.nb_medecins_generalistes, 0) > 0
                THEN COALESCE(b.nb_medecins_generalistes, 0) * 100.0 / 3
                WHEN COALESCE(z.est_zrr, FALSE) = TRUE
                THEN 0.0   -- Désert médical en zone rurale fragile
                ELSE 30.0  -- Pas de médecin dans l'IRIS mais commune non isolée
              END * 0.35

            -- Accès enseignement (25%) : écoles primaires + collèges/lycées
            -- Seuil : ≥ 2 établissements = score max
            + LEAST(
                100.0,
                (COALESCE(b.nb_ecoles_primaires, 0) + COALESCE(b.nb_colleges_lycees, 0)) * 50.0
              ) * 0.25
        )::NUMERIC,
        2
    ) AS score_gouvernance_0_100,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM ref_iris r
LEFT JOIN bpe         b ON r.iris_code = b.iris_code
LEFT JOIN zrr_communes z ON r.com_code  = z.com_code
