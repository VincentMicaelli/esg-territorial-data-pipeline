-- Test : le taux de pauvreté, quand renseigné, doit être dans [0, 100].
SELECT iris_code, taux_pauvrete_60
FROM {{ ref('gold_esg_iris_final') }}
WHERE taux_pauvrete_60 IS NOT NULL
  AND (taux_pauvrete_60 < 0 OR taux_pauvrete_60 > 100)