-- Dimension calendrier : une ligne par jour calendaire, de la première cotation disponible
-- (toutes sources confondues) jusqu'à aujourd'hui en UTC. Table recalculée à chaque build :
-- elle s'allonge d'un jour par jour écoulé, sans dépendre d'une nouvelle cotation.
-- Attributs ISO (lundi = 1) calculés avec les fonctions *iso de Snowflake, indépendantes
-- des paramètres de session WEEK_START et WEEK_OF_YEAR_POLICY.

with

first_quote_dates as (

    -- première date de chaque staging ; la borne basse est calculée, jamais codée en dur
    select min(quote_date) as first_date from {{ ref('stg_gold__boursorama') }}
    union all
    select min(quote_date) as first_date from {{ ref('stg_oil__eia') }}
    union all
    select min(quote_date) as first_date from {{ ref('stg_bitcoin__binance_vision') }}
    union all
    select min(quote_date) as first_date from {{ ref('stg_eurusd__ecb') }}

),

bounds as (

    select
        min(first_date)                                             as first_date,
        -- « aujourd'hui » en UTC, quelle que soit la timezone de la session
        convert_timezone('UTC', current_timestamp())::date          as today_utc
    from first_quote_dates

),

day_offsets as (

    -- row_number() donne une suite 0, 1, 2... sans trou (seq4() seul peut en avoir).
    -- 100000 jours (~273 ans) couvrent largement l'historique depuis la première
    -- cotation jusqu'à aujourd'hui ; le surplus est filtré plus bas.
    select
        row_number() over (order by seq4()) - 1                     as day_offset
    from table(generator(rowcount => 100000))

),

calendar as (

    select
        dateadd(day, o.day_offset, b.first_date)::date              as date_day
    from day_offsets as o
    cross join bounds as b
    where dateadd(day, o.day_offset, b.first_date) <= b.today_utc

),

final as (

    select
        date_day,
        year(date_day)                                              as year,
        quarter(date_day)                                           as quarter,
        month(date_day)                                             as month,
        -- nom complet du mois en anglais (January...December)
        to_char(date_day, 'MMMM')                                   as month_name,
        dayofmonth(date_day)                                        as day_of_month,
        -- ISO : 1 = lundi ... 7 = dimanche, indépendant de WEEK_START
        dayofweekiso(date_day)                                      as day_of_week,
        decode(
            dayofweekiso(date_day),
            1, 'Monday',
            2, 'Tuesday',
            3, 'Wednesday',
            4, 'Thursday',
            5, 'Friday',
            6, 'Saturday',
            7, 'Sunday'
        )                                                           as day_name,
        dayofweekiso(date_day) in (6, 7)                            as is_weekend,
        -- semaine ISO 8601 (1 à 53) et son année ISO, qui diffère de year autour du 1er janvier
        weekiso(date_day)                                           as iso_week,
        yearofweekiso(date_day)                                     as iso_year
    from calendar

)

select * from final
