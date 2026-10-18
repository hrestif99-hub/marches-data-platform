-- Test singulier : le libellé product, ajouté par le loader, doit correspondre à la série
-- EIA : RBRTE -> brent, RWTC -> wti. En error : une paire incohérente signale un bug du
-- loader (libellé mal attribué), qui fausserait tout filtrage par produit.
-- Un product NULL est aussi renvoyé (is distinct from) : il ne peut pas être cohérent.
-- Une série hors liste est contrôlée par accepted_values sur series, pas ici.
{{ config(severity='error') }}

with

expected as (

    select
        source,
        series,
        quote_date,
        product,
        case series
            when 'RBRTE' then 'brent'
            when 'RWTC' then 'wti'
        end                                                         as expected_product
    from {{ ref('stg_oil__eia') }}

)

select
    source,
    series,
    quote_date,
    product,
    expected_product
from expected
where expected_product is not null
    and product is distinct from expected_product
