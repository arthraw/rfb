select 
    empresa.COD_CNPJ_BASICO as COD_CNPJ_BASICO,
    empresa.DSC_RAZAO_SOCIAL as DSC_RAZAO_SOCIAL,
    empresa.DSC_PORTE_EMPRESA as DSC_PORTE_EMPRESA
from {{ ref('stg_empresa') }} as empresa