-- Test singulier : aucune incohérence OHLC (high < low, open ou close hors [low, high]).
-- En warn, par cohérence avec l'or : le loader refuse déjà les OHLC incohérents,
-- aucune ligne n'est attendue. Une ligne signalée (is_ohlc_inconsistent) est gardée telle
-- quelle en staging et non corrigée.
-- Le cas high < low, le plus grave, a aussi son propre test :
-- assert_stg_bitcoin__binance_vision_high_gte_low.
{{ config(severity='warn') }}

select
    source,
    symbol,
    quote_date,
    open_usdt,
    high_usdt,
    low_usdt,
    close_usdt,
    is_high_below_low,
    is_open_outside_range,
    is_close_outside_range
from {{ ref('stg_bitcoin__binance_vision') }}
where is_ohlc_inconsistent
