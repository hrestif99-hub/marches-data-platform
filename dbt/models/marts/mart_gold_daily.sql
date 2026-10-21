-- Cours journalier de l'or (une ligne par jour coté), avec les événements géopolitiques
-- rattachés au jour de cotation : nom_evenement et cours_evenement.
-- Rattachement : un événement va sur le dernier jour coté de l'or à sa date ou avant.
-- Un événement un jour sans cotation (ex. samedi 2023-10-07) est donc placé sur le
-- vendredi précédent, pour avoir un point sur la courbe de l'or.
-- Seuls les événements dont la date est couverte par l'historique de l'or sont rattachés
-- (available_assets contient gold) : un événement postérieur à la fin de la série ne doit
-- pas être collé sur la dernière cotation connue.

with

gold as (

    select
        quote_date,
        open_usd,
        high_usd,
        low_usd,
        price_usd,
        has_full_ohlc,
        is_ohlc_inconsistent
    from {{ ref('stg_gold__boursorama') }}

),

events as (

    select
        event_id,
        event_name,
        event_date
    from {{ ref('dim_events') }}
    where array_contains('gold'::variant, split(available_assets, ','))

),

events_on_gold_day as (

    -- dernier jour coté de l'or <= event_date
    select
        e.event_id,
        e.event_name,
        e.event_date,
        max(g.quote_date)                                           as quote_date
    from events as e
    inner join gold as g
        on g.quote_date <= e.event_date
    group by
        e.event_id,
        e.event_name,
        e.event_date

),

events_by_day as (

    -- une ligne par jour : plusieurs événements le même jour sont regroupés
    select
        quote_date,
        listagg(event_name, ' | ') within group (order by event_date, event_id)
                                                                    as nom_evenement,
        min(event_date)                                             as date_evenement,
        count(*)                                                    as nb_evenements
    from events_on_gold_day
    group by
        quote_date

),

final as (

    select
        g.quote_date,
        g.open_usd,
        g.high_usd,
        g.low_usd,
        g.price_usd,
        g.has_full_ohlc,
        g.is_ohlc_inconsistent,
        e.nom_evenement,
        -- cours de clôture du jour de rattachement, seulement les jours avec événement
        iff(e.quote_date is not null, g.price_usd, null)            as cours_evenement,
        e.date_evenement,
        coalesce(e.nb_evenements, 0)                                as nb_evenements
    from gold as g
    left join events_by_day as e
        on e.quote_date = g.quote_date

)

select * from final
