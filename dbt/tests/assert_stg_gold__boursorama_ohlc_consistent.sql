-- Test singulier : aucune incohérence OHLC (high < low, open ou close hors [low, high]).
-- En warn : la source en contient, gardées telles quelles en staging (signalées par
-- is_ohlc_inconsistent) et non corrigées. Chiffre daté : ingestion/gold/README.md.
-- Le cas high < low, le plus grave, a aussi son propre test : assert_stg_gold__boursorama_high_gte_low.
{{ config(severity='warn') }}

select
    source,
    symbol,
    quote_date,
    open_usd,
    high_usd,
    low_usd,
    price_usd,
    is_high_below_low,
    is_open_outside_range,
    is_close_outside_range
from {{ ref('stg_gold__boursorama') }}
where is_ohlc_inconsistent
