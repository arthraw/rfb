with source as (
    select * from {{ source('raw', 'raw_municipio_pib') }}
),
municipio_pib as (
    select
        try_cast(COD_MUNICIPIO as integer)          as COD_MUNICIPIO,
        upper(NOM_MUNICIPIO)                        as NOM_MUNICIPIO,
        coalesce(try_cast(VALOR as float), 0)       as VALOR
    from source
)
select * from municipio_pib
where COD_MUNICIPIO is not null