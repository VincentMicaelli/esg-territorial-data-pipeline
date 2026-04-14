{{
    config(
        materialized = 'table',
        tags = ['silver', 'referentiel', 'critique'],
        description = '''
        Referentiel territorial maitre : un IRIS = une ligne.
        Cle de jointure centrale de tout le modele de donnees.
        Construit depuis le fichier interne (S12)
        TOUT modele Silver/Gold doit joindre sur cette table pour garantir que seuls les IRIS internes sont inclus
        ''',
        meta = {
            'owner': 'data-engineering',
            'source_ids': ['S12'],
            'pk': 'iris_code',
            'sla': '< 5 minutes',
        }
    )
}}

WITH

source_interne AS (
    SELECT
        {{ normalize_iris_code('code_iris') }} AS iris_code,
        TRIM(COALESCE(lib_iris, ''))           AS iris_label,
        TRIM(code_bv)                          AS bv_code,
        TRIM(COALESCE(lib_bv, ''))             AS bv_label
    FROM {{ source('bronze', 'raw_internal_bv_iris') }}
    WHERE code_iris IS NOT NULL
        AND TRIM(CAST(code_iris AS VARCHAR)) != '' 
),

-- Dedup : en cas de doublon sur iris_code, garder la premiere ligne
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY iris_code
            ORDER BY iris_code
        ) AS rn
    FROM source_interne
    WHERE iris_code IS NOT NULL
),

final AS (
    SELECT
        iris_code,
        iris_label,
        bv_code,
        bv_label,
        -- Codes geo derive du code IRIS
        LEFT(iris_code, 2) AS dep_code,
        LEFT(iris_code, 5) AS com_code,
        -- Flag IRIS "commune entiere"
        (RIGHT(iris_code, 4) = '0000') AS is_commune_entiere,
        CURRENT_TIMESTAMP AS dbt_updated_at,
        '{{ var("version_modele") }}' AS version_modele
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM final
