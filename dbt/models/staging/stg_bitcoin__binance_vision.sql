-- Staging bitcoin (Binance Vision, bougies journalières BTCUSDT) : extraction et typage
-- des champs JSON, dédoublonnage. Prix en USDT, PAS en USD : aucune conversion.
-- quote_date est la date UTC d'ouverture de la bougie, calculée et contrôlée par le loader
-- (open_time n'est pas en RAW). Aucune correction des valeurs : les lignes incohérentes
-- sont seulement signalées.

with

source_rows as (

    select
        raw_data,
        file_name,
        file_row_number,
        loaded_at
    from {{ source('raw', 'BITCOIN_BINANCE_VISION') }}

),

typed as (

    select
        raw_data:source::varchar                                    as source,
        raw_data:symbol::varchar                                    as symbol,
        try_to_date(raw_data:quote_date::varchar, 'YYYY-MM-DD')     as quote_date,
        -- number(18,8) : 8 décimales = précision des CSV Binance (2 au plus observées,
        -- pas de cotation de 0,01 USDT), 10 chiffres entiers ; type exact à partir de RAW,
        -- mais le loader passe par un float Python (~15-17 chiffres significatifs) avant RAW
        raw_data:open_usdt::number(18, 8)                           as open_usdt,
        raw_data:high_usdt::number(18, 8)                           as high_usdt,
        raw_data:low_usdt::number(18, 8)                            as low_usdt,
        raw_data:close_usdt::number(18, 8)                          as close_usdt,
        raw_data:base_currency::varchar                             as base_currency,
        raw_data:quote_currency::varchar                            as quote_currency,
        -- number(20,8) : 8 décimales = le satoshi, 12 chiffres entiers (6 au plus observés)
        raw_data:volume_btc::number(20, 8)                          as volume_btc,
        -- number(28,8) : 8 décimales (8 au plus observées), 20 chiffres entiers (11 observés) ;
        -- ~19 chiffres significatifs : les dernières décimales sont déjà arrondies par le float du loader
        raw_data:quote_volume_usdt::number(28, 8)                   as quote_volume_usdt,
        raw_data:number_of_trades::number(38, 0)                    as number_of_trades,
        raw_data:raw_file::varchar                                  as raw_file,
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
        quote_date,
        open_usdt,
        high_usdt,
        low_usdt,
        close_usdt,
        base_currency,
        quote_currency,
        volume_btc,
        quote_volume_usdt,
        number_of_trades,
        raw_file,
        ingested_at,
        file_name,
        loaded_at,
        -- vrai OHLC disponible : les 4 valeurs présentes et pas une barre « plate »
        -- (open = high = low = close) ; même règle que l'or
        coalesce(
            open_usdt is not null
            and high_usdt is not null
            and low_usdt is not null
            and close_usdt is not null
            and not (open_usdt = close_usdt and high_usdt = close_usdt and low_usdt = close_usdt),
            false
        )                                                           as has_full_ohlc
    from deduplicated

),

flagged as (

    -- signaux de qualité : jamais NULL, false quand l'OHLC n'est pas complet
    -- (une ligne incomplète n'est pas incohérente, elle est incomplète)
    select
        *,
        coalesce(has_full_ohlc and high_usdt < low_usdt, false)     as is_high_below_low,
        coalesce(
            has_full_ohlc and open_usdt not between low_usdt and high_usdt, false
        )                                                           as is_open_outside_range,
        coalesce(
            has_full_ohlc and close_usdt not between low_usdt and high_usdt, false
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
