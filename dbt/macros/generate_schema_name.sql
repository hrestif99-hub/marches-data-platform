{#-
    Par défaut, dbt construit le schéma "<target_schema>_<custom_schema>" (ex. STAGING_MARTS).
    Ici, le schéma personnalisé (+schema dans dbt_project.yml) est utilisé tel quel,
    pour écrire dans les schémas existants MARCHES.STAGING et MARCHES.MARTS.
    Sans schéma personnalisé, on retombe sur le schéma de la cible (profiles.yml).
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
