-- Test : le score ESG synthetique ne doit jamais être NULL (il a une valeur par défaut 50).
SELECT iris_code
FROM {{ ref('gold_esg_iris_final') }}
WHERE score_esg_0_100 IS NULL