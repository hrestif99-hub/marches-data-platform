-- Test singulier : toute date de cotation des stagings existe dans dim_date, donc aucune
-- cotation n'est perdue par la jointure gauche de mart_prices_daily.
-- Cas visés : une date future (après aujourd'hui UTC, date mal calculée par un loader ou
-- décalage de fuseau), ou une date antérieure au début de dim_date.
-- Renvoie chaque cotation absente de dim_date. Les quote_date NULL sont ignorées : elles
-- relèvent des tests not_null des stagings.
-- En warn : la donnée n'est pas fausse en soi, elle est à examiner, pas à corriger ici.
{{ config(severity='warn') }}

with

quote_dates as (

    select
        'gold'                                                      as asset,
        quote_date
    from {{ ref('stg_gold__boursorama') }}

    union all

    select
        lower(series)                                               as asset,
        quote_date
    from {{ ref('stg_oil__eia') }}

    union all

    select
        'bitcoin'                                                   as asset,
        quote_date
    from {{ ref('stg_bitcoin__binance_vision') }}

    union all

    select
        'eurusd'                                                    as asset,
        quote_date
    from {{ ref('stg_eurusd__ecb') }}

)

select
    q.asset,
    q.quote_date
from quote_dates as q
left join {{ ref('dim_date') }} as d
    on d.date_day = q.quote_date
where q.quote_date is not null
    and d.date_day is null
