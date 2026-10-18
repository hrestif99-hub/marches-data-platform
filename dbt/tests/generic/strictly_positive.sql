{#-
    Test générique : la colonne est strictement positive (> 0).
    Renvoie les lignes où la valeur est <= 0 ; les NULL sont ignorés
    (ils relèvent d'un test not_null séparé).
    La sévérité se choisit dans le .yml du modèle, selon que la valeur négative
    est impossible (error) ou un événement de marché réel à signaler (warn).
    Usage dans un .yml de modèle :
        columns:
          - name: price
            data_tests:
              - strictly_positive:
                  config:
                    severity: warn
-#}
{% test strictly_positive(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
