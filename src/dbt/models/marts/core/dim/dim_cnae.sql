select 
    cnae.COD_CNAE as COD_CNAE,
    cnae.DSC_CNAE as DSC_CNAE
from {{ ref('stg_cnae') }} as cnae