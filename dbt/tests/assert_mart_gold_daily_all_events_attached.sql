-- Test singulier : chaque événement couvert par l'historique de l'or est rattaché à un
-- jour de mart_gold_daily, et seulement à un jour antérieur ou égal à sa date.
-- Renvoie une ligne si le total rattaché diffère du nombre d'événements attendus,
-- ou si un rattachement tombe après la date de l'événement.

with

expected as (

    select count(*) as n
    from {{ ref('dim_events') }}
    where array_contains('gold'::variant, split(available_assets, ','))

),

attached as (

    select coalesce(sum(nb_evenements), 0) as n
    from {{ ref('mart_gold_daily') }}

)

select
    'count_mismatch' as issue,
    null as quote_date
from expected, attached
where expected.n <> attached.n

union all

select
    'attached_after_event' as issue,
    quote_date
from {{ ref('mart_gold_daily') }}
where date_evenement < quote_date
