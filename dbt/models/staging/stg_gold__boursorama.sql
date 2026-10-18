-- Staging or (Boursorama) : extraction et typage des champs JSON, dédoublonnage.
-- Aucune correction des valeurs : les lignes incohérentes sont seulement signalées.

with

source_rows as (

    select
        raw_data,
        file_name,
        file_row_number,
        loaded_at
    from {{ source('raw', 'GOLD_BOURSORAMA') }}

),

typed as (

    select
        raw_data:source::varchar                                    as source,
        raw_data:symbol::varchar                                    as symbol,
        raw_data:instrument_name::varchar                           as instrument_name,
        try_to_date(raw_data:quote_date::varchar, 'YYYY-MM-DD')     as quote_date,
        -- number(18,4) : 4 décimales au plus observées dans la source, cours < 10^14 ;
        -- type exact (pas de float) pour comparer et agréger sans erreur d'arrondi
        raw_data:open_usd::number(18, 4)                            as open_usd,
        raw_data:high_usd::number(18, 4)                            as high_usd,
        raw_data:low_usd::number(18, 4)                             as low_usd,
        -- cours de clôture (champ "c" de Boursorama)
        raw_data:price_usd::number(18, 4)                           as price_usd,
        raw_data:volume::number(38, 0)                              as volume,
        raw_data:currency::varchar                                  as currency,
        raw_data:unit::varchar                                      as unit,
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

    -- une ligne par (source, symbol, quote_date) : la plus récemment collectée puis chargée
    select *
    from typed
    qualify row_number() over (
        partition by
            source,
            symbol,
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
        symbol,
        instrument_name,
        quote_date,
        open_usd,
        high_usd,
        low_usd,
        price_usd,
        volume,
        currency,
        unit,
        ingested_at,
        file_name,
        loaded_at,
        -- vrai OHLC disponible : les 4 valeurs présentes et pas une barre « plate »
        -- (open = high = low = close), forme des lignes clôture seule de l'historique ancien
        coalesce(
            open_usd is not null
            and high_usd is not null
            and low_usd is not null
            and price_usd is not null
            and not (open_usd = price_usd and high_usd = price_usd and low_usd = price_usd),
            false
        )                                                           as has_full_ohlc
    from deduplicated

),

flagged as (

    -- signaux de qualité : jamais NULL, false quand l'OHLC n'est pas complet
    -- (une ligne clôture seule n'est pas incohérente, elle est incomplète)
    select
        *,
        coalesce(has_full_ohlc and high_usd < low_usd, false)       as is_high_below_low,
        coalesce(
            has_full_ohlc and open_usd not between low_usd and high_usd, false
        )                                                           as is_open_outside_range,
        coalesce(
            has_full_ohlc and price_usd not between low_usd and high_usd, false
        )                                                           as is_close_outside_range
    from final

),

with_global_flag as (

    -- drapeau global : au moins une des trois incohérences OHLC
    select
        *,
        is_high_below_low
            or is_open_outside_range
            or is_close_outside_range                               as is_ohlc_inconsistent
    from flagged

)

select * from with_global_flag
