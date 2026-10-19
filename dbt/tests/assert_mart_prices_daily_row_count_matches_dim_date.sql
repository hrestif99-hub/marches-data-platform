-- Test singulier : mart_prices_daily a exactement autant de lignes que dim_date.
-- Moins de lignes = jours perdus ; plus de lignes = jointure qui duplique (un staging
-- non unique par date, ou le pétrole joint sans filtre sur la série).
-- En error : le mart doit avoir une ligne par jour de dim_date, ni plus ni moins.
{{ config(severity='error') }}

with

mart_count as (

    select
        count(*)                                                    as row_count
    from {{ ref('mart_prices_daily') }}

),

dim_count as (

    select
        count(*)                                                    as row_count
    from {{ ref('dim_date') }}

)

select
    m.row_count                                                     as mart_row_count,
    d.row_count                                                     as dim_date_row_count
from mart_count as m
cross join dim_count as d
where m.row_count != d.row_count
