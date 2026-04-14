-- Test : aucun iris_code ne doit apparaître plus d'une fois dans la table finale.
-- Résultat attendu : 0 lignes (test PASS si 0 lignes retournées)
SELECT
    iris_code,
    COUNT(*) AS occurrences
FROM {{ ref('gold_esg_iris_final') }}
GROUP BY iris_code
HAVING COUNT(*) > 1