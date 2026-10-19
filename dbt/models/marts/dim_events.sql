-- Dimension des événements géopolitiques : le seed geopolitical_events, enrichi de
-- available_assets (actifs dont l'historique, du début à la fin, couvre la date de
-- l'événement) et de
-- first_trading_date (premier jour, à partir de la date de l'événement, où tous ces
-- actifs ont coté).
-- Les bornes d'historique sont calculées depuis mart_prices_daily, jamais codées en dur :
-- ce sont les mêmes drapeaux has_* qui servent ensuite à trouver le premier jour commun.
-- Le pétrole est représenté par le Brent, comme pour is_common_trading_day du mart.
-- ATTENTION : l'or, le Brent et la BCE suivent des calendriers différents (marchés US,
-- Londres, TARGET) ; le premier jour commun est donc le premier jour ouvré de TOUS ces
-- calendriers à la fois, pas seulement le premier jour hors week-end.

with

events as (

    select
        event_id,
        event_name,
        event_date,
        category,
        description,
        source_url
    from {{ ref('geopolitical_events') }}

),

prices as (

    select
        date_day,
        has_gold,
        has_brent,
        has_btc,
        has_eurusd
    from {{ ref('mart_prices_daily') }}

),

asset_date_ranges as (

    -- historique de chaque actif : première ET dernière date avec une cotation.
    -- La fin compte autant que le début : l'or vient d'un scraping unique et sa série
    -- s'arrête ; un événement postérieur à la fin d'une série ne doit plus l'exiger.
    select
        min(case when has_gold then date_day end)                   as gold_first_date,
        max(case when has_gold then date_day end)                   as gold_last_date,
        min(case when has_brent then date_day end)                  as oil_first_date,
        max(case when has_brent then date_day end)                  as oil_last_date,
        min(case when has_eurusd then date_day end)                 as eurusd_first_date,
        max(case when has_eurusd then date_day end)                 as eurusd_last_date,
        min(case when has_btc then date_day end)                    as btc_first_date,
        max(case when has_btc then date_day end)                    as btc_last_date
    from prices

),

events_with_coverage as (

    -- un actif est « disponible » si son historique couvre le jour de l'événement :
    -- first_date <= event_date <= last_date ; ce n'est PAS une cotation ce jour-là précis.
    -- coalesce(..., false) : un actif sans aucune cotation n'est jamais disponible
    select
        e.*,
        coalesce(
            e.event_date between r.gold_first_date and r.gold_last_date,
            false
        )                                                           as is_gold_available,
        coalesce(
            e.event_date between r.oil_first_date and r.oil_last_date,
            false
        )                                                           as is_oil_available,
        coalesce(
            e.event_date between r.eurusd_first_date and r.eurusd_last_date,
            false
        )                                                           as is_eurusd_available,
        coalesce(
            e.event_date between r.btc_first_date and r.btc_last_date,
            false
        )                                                           as is_btc_available
    from events as e
    cross join asset_date_ranges as r

),

first_trading_dates as (

    -- premier jour >= event_date où chaque actif disponible a une cotation ;
    -- un actif non disponible n'est pas exigé (ex. bitcoin avant le début de BTCUSDT)
    select
        e.event_id,
        min(p.date_day)                                             as first_trading_date
    from events_with_coverage as e
    inner join prices as p
        on p.date_day >= e.event_date
        and (not e.is_gold_available or p.has_gold)
        and (not e.is_oil_available or p.has_brent)
        and (not e.is_eurusd_available or p.has_eurusd)
        and (not e.is_btc_available or p.has_btc)
    group by
        e.event_id

),

final as (

    select
        e.event_id,
        e.event_name,
        e.event_date,
        -- ISO : 1 = lundi ... 7 = dimanche, comme dim_date
        dayofweekiso(e.event_date)                                  as event_day_of_week,
        decode(
            dayofweekiso(e.event_date),
            1, 'Monday',
            2, 'Tuesday',
            3, 'Wednesday',
            4, 'Thursday',
            5, 'Friday',
            6, 'Saturday',
            7, 'Sunday'
        )                                                           as event_day_name,
        e.category,
        e.description,
        e.source_url,
        -- ordre fixe et documenté : gold, oil, eurusd, btc, quel que soit l'événement ;
        -- array_construct_compact retire les NULL : seuls les actifs disponibles restent
        array_to_string(
            array_construct_compact(
                iff(e.is_gold_available, 'gold', null),
                iff(e.is_oil_available, 'oil', null),
                iff(e.is_eurusd_available, 'eurusd', null),
                iff(e.is_btc_available, 'btc', null)
            ),
            ','
        )                                                           as available_assets,
        t.first_trading_date,
        datediff(day, e.event_date, t.first_trading_date)           as days_to_first_trading_date
    from events_with_coverage as e
    left join first_trading_dates as t
        on t.event_id = e.event_id

)

select * from final
