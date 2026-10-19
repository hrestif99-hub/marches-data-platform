-- Prix journaliers des quatre actifs côte à côte : une ligne par jour de dim_date.
-- Les prix, drapeaux et has_* viennent tels quels de int_prices_daily (voir ce modèle
-- pour les jointures, les heures de clôture et la sémantique NULL des drapeaux).
-- Ajout : les événements de dim_events, rattachés à leur first_trading_date (premier jour
-- de cotation commun), PAS forcément à leur date calendaire event_date.
-- Une ligne par date garantie : les événements sont agrégés par first_trading_date AVANT
-- la jointure gauche. Plusieurs événements le même jour -> libellés séparés par ' | ',
-- dans l'ordre (event_date, event_id) ; event_count compte les événements (0 sinon).

with

prices as (

    select
        *
    from {{ ref('int_prices_daily') }}

),

events as (

    -- les événements sans first_trading_date ne sont rattachés à aucun jour ;
    -- ils sont détectés par not_null sur dim_events.first_trading_date et par
    -- assert_mart_prices_daily_all_events_attached
    select
        event_id,
        event_name,
        event_date,
        category,
        first_trading_date,
        row_number() over (
            partition by first_trading_date
            order by event_date, event_id
        )                                                           as event_rank
    from {{ ref('dim_events') }}
    where first_trading_date is not null

),

names_by_date as (

    -- un libellé par événement (pas de distinct) : autant de libellés que event_count
    select
        first_trading_date,
        listagg(event_name, ' | ')
            within group (order by event_rank)                      as event_name,
        count(*)                                                    as event_count
    from events
    group by
        first_trading_date

),

categories as (

    -- dédoublonnage des catégories AVANT listagg : Snowflake refuse
    -- listagg(distinct x) trié sur une autre colonne que x. Chaque catégorie garde
    -- le rang de son premier événement, pour conserver l'ordre (event_date, event_id).
    select
        first_trading_date,
        category,
        min(event_rank)                                             as first_event_rank
    from events
    group by
        first_trading_date,
        category

),

categories_by_date as (

    select
        first_trading_date,
        listagg(category, ' | ')
            within group (order by first_event_rank)                as event_category
    from categories
    group by
        first_trading_date

),

final as (

    select
        p.*,
        -- NULL les jours sans événement
        n.event_name,
        c.event_category,
        -- 0 les jours sans événement, jamais NULL
        coalesce(n.event_count, 0)                                  as event_count
    from prices as p
    left join names_by_date as n
        on n.first_trading_date = p.date_day
    left join categories_by_date as c
        on c.first_trading_date = p.date_day

)

select * from final
