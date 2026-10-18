-- Staging pétrole (EIA, prix spot Brent RBRTE et WTI RWTC) : extraction et typage
-- des champs JSON, dédoublonnage. Aucune correction des valeurs : un prix <= 0
-- (ex. WTI d'avril 2020) est gardé tel quel et seulement signalé.

with

source_rows as (

    select
        raw_data,
        file_name,
        file_row_number,
        loaded_at
    from {{ source('raw', 'OIL_EIA') }}

),

typed as (

    select
        raw_data:source::varchar                                    as source,
        -- identifiant de série EIA (RBRTE = Brent, RWTC = WTI), champ JSON series_id
        raw_data:series_id::varchar                                 as series,
        raw_data:product::varchar                                   as product,
        raw_data:product_name::varchar                              as product_name,
        raw_data:series_description::varchar                        as series_description,
        try_to_date(raw_data:quote_date::varchar, 'YYYY-MM-DD')     as quote_date,
        -- number(18,4) : 2 décimales au plus observées dans la source, 4 gardées par marge
        -- et par cohérence avec l'or ; type exact (pas de float) pour comparer et agréger
        raw_data:price_usd::number(18, 4)                           as price_usd_per_barrel,
        raw_data:currency::varchar                                  as currency,
        raw_data:unit::varchar                                      as unit,
        raw_data:raw_units::varchar                                 as raw_units,
        -- chaîne ISO UTC terminée par Z : format explicite, résultat en UTC
        try_to_timestamp_ntz(
            raw_data:ingested_at::varchar,
            'YYYY-MM-DD"T"HH24:MI:SS"Z"'
        )                                                           as ingested_at,
        file_name,
        file_row_number,
        loaded_at
    from source_rows

),

deduplicated as (

    -- une ligne par (source, series, quote_date) : la plus récemment collectée puis chargée
    select *
    from typed
    qualify row_number() over (
        partition by
            source,
            series,
            quote_date
        order by
            -- nulls last : une date mal formée (NULL) ne doit pas gagner contre une ligne valide
            ingested_at desc nulls last,
            loaded_at desc nulls last,
            file_name desc,
            file_row_number desc
    ) = 1

),

final as (

    select
        source,
        series,
        product,
        product_name,
        series_description,
        quote_date,
        price_usd_per_barrel,
        currency,
        unit,
        raw_units,
        ingested_at,
        file_name,
        loaded_at,
        -- signal de qualité, jamais NULL : prix nul ou négatif (possible sur le marché
        -- physique, cf. WTI d'avril 2020), gardé tel quel
        coalesce(price_usd_per_barrel <= 0, false)                  as is_price_non_positive
    from deduplicated

)

select * from final
