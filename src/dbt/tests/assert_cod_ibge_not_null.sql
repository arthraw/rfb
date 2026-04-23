{{ config(tags=['silver', 'stg_municipios_rfb']) }}

SELECT COD_MUNICIPIO_IBGE
FROM {{ ref('stg_municipios_rfb') }}
WHERE COD_MUNICIPIO_IBGE IS NULL