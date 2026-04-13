{{ config(tags=['silver', 'stg_estabelecimento']) }}

SELECT cod_cnpj
FROM {{ ref('stg_estabelecimento') }}
WHERE cod_cnpj IS NULL