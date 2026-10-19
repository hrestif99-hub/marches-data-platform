-- Prix journaliers des quatre actifs côte à côte : une ligne par jour de dim_date,
-- jointure gauche sur chaque staging. Rien n'est filtré ni exclu : un actif qui ne cote
-- pas ce jour-là a des colonnes NULL.
-- ATTENTION : une même date_day ne correspond pas au même instant d'un actif à l'autre
-- (bougie UTC pour le bitcoin, fixing BCE à Francfort, clôtures de marché pour l'or et le
-- pétrole). Le bitcoin est en USDT, pas en USD : aucune conversion n'est faite.
-- Drapeaux de qualité : NULL = actif absent ce jour-là (pas de cotation), false = cotation
-- présente sans anomalie. Les drapeaux de présence (has_*) et is_common_trading_day ne
-- sont jamais NULL.

with

dates as (

    select
        date_day
    from {{ ref('dim_date') }}

),

gold as (

    -- une ligne par quote_date (une seule source et un seul symbole en staging)
    select
        quote_date,
        price_usd,
        has_full_ohlc,
        is_ohlc_inconsistent
    from {{ ref('stg_gold__boursorama') }}

),

brent as (

    -- staging unique par (series, quote_date) : filtrer la série AVANT de joindre
    -- pour garder une ligne par date
    select
        quote_date,
        price_usd_per_barrel
    from {{ ref('stg_oil__eia') }}
    where series = 'RBRTE'

),

wti as (

    select
        quote_date,
        price_usd_per_barrel,
        is_price_non_positive
    from {{ ref('stg_oil__eia') }}
    where series = 'RWTC'

),

bitcoin as (

    select
        quote_date,
        close_usdt
    from {{ ref('stg_bitcoin__binance_vision') }}

),

eurusd as (

    select
        quote_date,
        usd_per_eur
    from {{ ref('stg_eurusd__ecb') }}

),

joined as (

    select
        d.date_day,
        g.price_usd                                                 as gold_price_usd,
        b.price_usd_per_barrel                                      as brent_usd_per_barrel,
        w.price_usd_per_barrel                                      as wti_usd_per_barrel,
        -- USDT, pas USD
        btc.close_usdt                                              as btc_close_usdt,
        e.usd_per_eur,
        -- drapeaux par actif : NULL si pas de ligne ce jour-là (jointure gauche)
        g.is_ohlc_inconsistent                                      as gold_is_ohlc_inconsistent,
        g.has_full_ohlc                                             as gold_has_full_ohlc,
        w.is_price_non_positive                                     as wti_is_price_non_positive,
        -- présence d'une ligne de staging ce jour-là, jamais NULL
        g.quote_date is not null                                    as has_gold,
        b.quote_date is not null                                    as has_brent,
        w.quote_date is not null                                    as has_wti,
        btc.quote_date is not null                                  as has_btc,
        e.quote_date is not null                                    as has_eurusd
    from dates as d
    left join gold as g
        on g.quote_date = d.date_day
    left join brent as b
        on b.quote_date = d.date_day
    left join wti as w
        on w.quote_date = d.date_day
    left join bitcoin as btc
        on btc.quote_date = d.date_day
    left join eurusd as e
        on e.quote_date = d.date_day

),

final as (

    select
        *,
        -- les quatre actifs ont un prix ce jour-là (Brent pour le pétrole) ; jamais NULL
        gold_price_usd is not null
            and brent_usd_per_barrel is not null
            and btc_close_usdt is not null
            and usd_per_eur is not null                             as is_common_trading_day
    from joined

)

select * from final
