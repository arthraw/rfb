with source as (
    select * from {{ source('raw', 'raw_municipios_rfb') }}
),
municipios_rfb as (
    select
        try_cast(cod_municipio_rfb as integer)              as COD_MUNICIPIO_RFB,
        try_cast(cod_municipio_ibge as integer)             as COD_MUNICIPIO_IBGE,
        upper(nome_municipio_rfb)                           as NOM_MUNICIPIO_RFB,
        upper(nome_municipio_ibge)                          as NOM_MUNICIPIO_IBGE,
        upper(uf)                                           as DSC_UF
    from source
)
select * from municipios_rfb