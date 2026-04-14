-- Test : chaque IRIS du ref doit avoir un score ESG dans la table finale.
SELECT r.iris_code
FROM {{ ref('silver_referentiel_iris') }} r
LEFT JOIN {{ ref('gold_esg_iris_final') }} g ON r.iris_code = g.iris_code
WHERE g.iris_code IS NULL