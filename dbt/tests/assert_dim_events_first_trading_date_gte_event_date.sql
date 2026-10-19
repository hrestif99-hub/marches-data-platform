-- Test singulier : le premier jour de bourse commun ne précède jamais l'événement.
-- Renvoie les événements où first_trading_date < event_date. Les NULL sont ignorés :
-- ils relèvent du test not_null sur first_trading_date.
-- En error : une date antérieure fausserait toute mesure de réaction des marchés.
{{ config(severity='error') }}

select
    event_id,
    event_date,
    first_trading_date
from {{ ref('dim_events') }}
where first_trading_date < event_date
