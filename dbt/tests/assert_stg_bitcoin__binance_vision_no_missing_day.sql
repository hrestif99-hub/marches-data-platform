-- Test singulier : le bitcoin cote 7j/7, il doit y avoir une ligne par jour calendaire
-- entre la première et la dernière quote_date (cf. CLAUDE.md, « Qualité des données »).
-- Renvoie chaque jour manquant. En warn : le loader tolère quelques trous (MAX_MISSING_DAYS)
-- en les loggant ; un trou est à examiner, pas à combler ici.
{{ config(severity='warn') }}

with bounds as (

    select
        symbol,
        min(quote_date) as first_date,
        max(quote_date) as last_date
    from {{ ref('stg_bitcoin__binance_vision') }}
    group by symbol

),

calendar as (

    -- un jour par ligne entre first_date et last_date (row_number() sans trou, contrairement à seq4()) ;
    -- 20000 jours (~54 ans depuis 2017) couvrent largement l'historique BTCUSDT
    select
        b.symbol,
        dateadd(day, row_number() over (partition by b.symbol order by g.seq) - 1, b.first_date) as expected_date,
        b.last_date
    from bounds as b
    cross join (select seq4() as seq from table(generator(rowcount => 20000))) as g

)

select
    c.symbol,
    c.expected_date as missing_date
from calendar as c
left join {{ ref('stg_bitcoin__binance_vision') }} as s
    on s.symbol = c.symbol
    and s.quote_date = c.expected_date
where c.expected_date <= c.last_date
    and s.quote_date is null
