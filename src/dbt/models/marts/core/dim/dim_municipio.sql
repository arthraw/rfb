select
    mun.COD_MUNICIPIO_RFB as COD_MUNICIPIO,
    mun.NOM_MUNICIPIO as NOM_MUNICIPIO,
    mun.DSC_UF as DSC_UF
from {{ ref('int_municipio_joined_to_pib') }} as mun 