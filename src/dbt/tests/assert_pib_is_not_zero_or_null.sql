{{ config(tags=['silver', 'stg_municipio_pib']) }}
select
    VALOR
from {{ ref('stg_municipio_pib') }}
where VALOR is null or VALOR = 0