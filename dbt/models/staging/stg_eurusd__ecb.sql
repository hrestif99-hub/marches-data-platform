-- Staging EUR/USD (taux de référence BCE, USD pour 1 EUR) : extraction et typage
-- des champs JSON, dédoublonnage. Aucune correction des valeurs.

with

source_rows as (

    select
        raw_data,
        file_name,
        file_row_number,
        loaded_at
    from {{ source('raw', 'EURUSD_ECB') }}

),

typed as (

    select
        raw_data:source::varchar                                    as source,
        raw_data:series_key::varchar                                as series_key,
        try_to_date(raw_data:quote_date::varchar, 'YYYY-MM-DD')     as quote_date,
        -- number(10,4) : la BCE publie le taux avec 4 décimales au plus, valeur de l'ordre
        -- de 1 ; type exact (pas de float) pour comparer et agréger sans erreur d'arrondi
        raw_data:rate_usd_per_eur::number(10, 4)                    as usd_per_eur,
        raw_data:base_currency::varchar                             as base_currency,
        raw_data:quote_currency::varchar                            as quote_currency,
        raw_data:unit::varchar                                      as unit,
        -- statut d'observation SDMX de la BCE (A = valeur normale)
        raw_data:obs_status::varchar                                as obs_status,
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

    -- une ligne par (source, quote_date) : la plus récemment collectée puis chargée
    select *
    from typed
    qualify row_number() over (
        partition by
            source,
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
        series_key,
        quote_date,
        usd_per_eur,
        base_currency,
        quote_currency,
        unit,
        obs_status,
        ingested_at,
        file_name,
        loaded_at
    from deduplicated

)

select * from final
