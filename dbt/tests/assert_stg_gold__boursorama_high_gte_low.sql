-- Test singulier : le plus haut doit être >= au plus bas.
-- En warn : la source contient au moins une ligne incohérente, gardée telle quelle
-- en staging (signalée par is_high_below_low) et non corrigée.
{{ config(severity='warn') }}

select
    source,
    symbol,
    quote_date,
    high_usd,
    low_usd
from {{ ref('stg_gold__boursorama') }}
where is_high_below_low
