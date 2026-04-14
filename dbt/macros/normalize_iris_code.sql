{#
  Normalise un code IRIS sur 9 caractères (format INSEE standard).
  Exemples :
    '75056101' → '750561010'  (8 chars → padded to 9)
    '010010000' → '010010000' (déjà 9 chars, inchangé)
    NULL → NULL

  Format IRIS : 2 (dépt) + 3 (commune) + 4 (numéro IRIS) = 9 chars total
  Pour les communes non découpées : dernier quartet = '0000'
#}
{% macro normalize_iris_code(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL OR TRIM(CAST({{ column_name }} AS VARCHAR)) = ''
        THEN NULL
        ELSE LPAD(TRIM(CAST({{ column_name }} AS VARCHAR)), 9, '0')
    END
{% endmacro %}


{#
  Extrait le code département (2 premiers chars) d'un code IRIS normalisé.
  Exemple: '750561010' → '75'
#}
{% macro iris_to_dep(iris_column) %}
    LEFT({{ normalize_iris_code(iris_column) }}, 2)
{% endmacro %}


{#
  Extrait le code commune (5 premiers chars) d'un code IRIS normalisé.
  Exemple: '750561010' → '75056'
#}
{% macro iris_to_commune(iris_column) %}
    LEFT({{ normalize_iris_code(iris_column) }}, 5)
{% endmacro %}


{#
  Convertit une valeur INSEE potentiellement secrete en NULL.
  INSEE utilise 's' (secret) ou 'nd' (non disponible) quand < 11 ménages.
  Appelé avant tout CAST sur les colonnes numériques FILOSOFI/RP.
#}
{% macro nullify_insee_secret(column_name) %}
    CASE
        WHEN UPPER(TRIM(CAST({{ column_name }} AS VARCHAR))) IN ('S', 'NS', 'ND', 'NC', 'N.D.', 'N/A', '')
        THEN NULL
        ELSE {{ column_name }}
    END
{% endmacro %}


{#
  Cast sécurisé vers NUMERIC après nettoyage des secrets INSEE.
  Gère aussi les virgules décimales (format français).
#}
{% macro safe_numeric(column_name, precision=10, scale=2) %}
    CASE
        WHEN UPPER(TRIM(CAST({{ column_name }} AS VARCHAR))) IN ('S', 'NS', 'ND', 'NC', 'N.D.', 'N/A', '')
        THEN NULL
        ELSE CAST(REPLACE(TRIM(CAST({{ column_name }} AS VARCHAR)), ',', '.') AS NUMERIC({{ precision }}, {{ scale }}))
    END
{% endmacro %}
