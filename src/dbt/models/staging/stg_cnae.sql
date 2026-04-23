with cnaes as (
    select
        codigo_cnae as COD_CNAE,
        descricao_cnae as DSC_CNAE
    from {{ source('raw', 'raw_cnae') }}
)
select * from cnaes