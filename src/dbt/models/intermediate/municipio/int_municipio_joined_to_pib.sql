with rfb as (
    select * from {{ ref('stg_municipios_rfb') }}
),
pib as (
    select * from {{ ref('stg_municipio_pib') }}
),
municipio_pib as (
    select
        rfb.COD_MUNICIPIO_RFB as COD_MUNICIPIO_RFB,
        pib.NOM_MUNICIPIO as NOM_MUNICIPIO,
        rfb.DSC_UF as DSC_UF,
        pib.VALOR as PIB
    from rfb
    left join pib on rfb.COD_MUNICIPIO_IBGE = pib.COD_MUNICIPIO
)
select * from municipio_pib