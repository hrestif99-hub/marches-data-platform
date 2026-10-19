-- Test singulier : aucun événement de dim_events n'est perdu dans mart_prices_daily.
-- sum(event_count) du mart doit être égal au nombre de lignes de dim_events.
-- Moins = événement non rattaché (first_trading_date NULL, ou hors de dim_date) ;
-- plus = événement compté deux fois (jointure qui duplique).
-- En error : le mart doit porter chaque événement exactement une fois.
{{ config(severity='error') }}

with

attached_events as (

    select
        coalesce(sum(event_count), 0)                               as event_count
    from {{ ref('mart_prices_daily') }}

),

all_events as (

    select
        count(*)                                                    as event_count
    from {{ ref('dim_events') }}

)

select
    a.event_count                                                   as attached_event_count,
    e.event_count                                                   as dim_events_count
from attached_events as a
cross join all_events as e
where a.event_count != e.event_count
