-- Test singulier : le plus haut doit être >= au plus bas.
-- En warn, par cohérence avec l'or : le loader refuse déjà les OHLC incohérents,
-- aucune ligne n'est attendue. Une ligne signalée (is_high_below_low) est gardée telle
-- quelle en staging et non corrigée.
{{ config(severity='warn') }}

select
    source,
    symbol,
    quote_date,
    high_usdt,
    low_usdt
from {{ ref('stg_bitcoin__binance_vision') }}
where is_high_below_low
