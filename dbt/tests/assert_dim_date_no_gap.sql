-- Test singulier : dim_date n'a aucun trou, une ligne par jour entre la première et la
-- dernière date. Avec unique sur date_day, count(*) = datediff + 1 garantit la continuité.
-- En error : un trou fausserait toute jointure ou tout contrôle de dates manquantes.
{{ config(severity='error') }}

with

summary as (

    select
        count(*)                                                    as row_count,
        min(date_day)                                               as first_date,
        max(date_day)                                               as last_date
    from {{ ref('dim_date') }}

)

select
    row_count,
    first_date,
    last_date,
    datediff(day, first_date, last_date) + 1                        as expected_row_count
from summary
where row_count != datediff(day, first_date, last_date) + 1
