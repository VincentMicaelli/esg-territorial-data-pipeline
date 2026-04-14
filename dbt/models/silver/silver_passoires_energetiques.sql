{{
  config(
    materialized='table',
    tags=['silver', 'env', 'passoires'],
    description='''
    Passoires énergétiques 2022 normalisées à la maille IRIS.
    Source S09 fournie en GeoPackage, aplatie en Bronze puis harmonisée ici.
    ''',
    meta={
      'source_ids': ['S09'],
      'millesime': '2022',
      'pk': 'iris_code'
    }
  )
}}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'raw_passoires_energetiques_iris') }}
),

cleaned AS (
    SELECT
        {{ normalize_iris_code('district') }} AS iris_code,
        LPAD(TRIM(CAST(city AS VARCHAR)), 5, '0') AS com_code,
        TRIM(CAST(city_name AS VARCHAR)) AS commune_label,
        TRIM(CAST(department AS VARCHAR)) AS dep_code,
        CAST(NULLIF(TRIM(CAST(nombre AS VARCHAR)), '') AS NUMERIC(14, 4)) AS nb_passoires_energetiques_estime,
        CAST(NULLIF(TRIM(CAST(taux AS VARCHAR)), '') AS NUMERIC(7, 4)) AS taux_passoires_energetiques_pct,
        CAST(NULLIF(TRIM(CAST(total AS VARCHAR)), '') AS NUMERIC(14, 4)) AS nb_residences_principales_estime,
        '{{ var("millesime_passoires") }}' AS millesime,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM raw
    WHERE district IS NOT NULL
      AND TRIM(CAST(district AS VARCHAR)) != ''
)

SELECT c.*
FROM cleaned c
INNER JOIN {{ ref('silver_referentiel_iris') }} r
    ON c.iris_code = r.iris_code
