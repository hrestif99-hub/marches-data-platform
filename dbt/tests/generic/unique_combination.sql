{#-
    Test générique : la combinaison des colonnes données est unique.
    Équivalent minimal de dbt_utils.unique_combination_of_columns, sans dépendance.
    Renvoie les combinaisons en double (le test échoue s'il en trouve).
    Usage dans un .yml de modèle :
        data_tests:
          - unique_combination:
              arguments:
                combination_of_columns: [col_a, col_b]
-#}
{% test unique_combination(model, combination_of_columns) %}

with

grouped as (

    select
        {{ combination_of_columns | join(',\n        ') }},
        count(*) as row_count
    from {{ model }}
    group by
        {{ combination_of_columns | join(',\n        ') }}

)

select *
from grouped
where row_count > 1

{% endtest %}
