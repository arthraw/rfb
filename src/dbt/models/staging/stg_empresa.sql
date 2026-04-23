with empresa as (
    select
        cnpj_basico as COD_CNPJ_BASICO,
        upper(razao_social) as DSC_RAZAO_SOCIAL,
        cast(natureza_juridica as string) as DSC_NATUREZA_JURIDICA,
        cast(qualificacao_responsavel as string) as DSC_QUALIFICACAO_RESPONSAVEL,
        cast(replace(capital_social, ',', '.') as float) as VLR_CAPITAL_SOCIAL,
        case
            when porte_empresa = '00' then 'NÃO INFORMADO'
            when porte_empresa = '01' then 'MICRO EMPRESA'
            when porte_empresa = '03' then 'EMPRESA DE PEQUENO PORTE'
            when porte_empresa = '05' then 'DEMAIS'
        end as DSC_PORTE_EMPRESA,
        ente_federativo_responsavel as DSC_ENTE_FEDERATIVO_RESPONSAVEL
    from {{ source("raw", "raw_empresa") }}
)
select * from empresa