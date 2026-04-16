with
dim_municipio as (
    select
        COD_MUNICIPIO,
        NOM_MUNICIPIO,
        DSC_UF
    from {{ ref('dim_municipio') }}
),
dim_cnae as (
    select
        COD_CNAE,
        DSC_CNAE
    from {{ ref('dim_cnae') }}
),
dim_empresa as (
    select
        e.COD_CNPJ_BASICO as COD_CNPJ_BASICO,
        e.DSC_RAZAO_SOCIAL as DSC_RAZAO_SOCIAL,
        e.DSC_PORTE_EMPRESA as DSC_PORTE_EMPRESA
    from {{ ref('dim_empresa') }} e
),
dim_estabelecimento as (
    select
        COD_CNPJ,
        COD_EMPRESA,
        COD_MUNICIPIO,
        COD_CNAE_PRINCIPAL,
        DAT_INICIO_ATIVIDADE
    from {{ ref('dim_estabelecimento') }}
),
dim_calendario as (
    select
        DAT_INICIO_ATIVIDADE,
        NUM_ANO,
        NUM_MES,
        NOM_MES,
        NUM_TRIMESTRE,
        NUM_SEMESTRE,
        FLG_FINAL_SEMANA
    from {{ ref('dim_calendario') }}
)
select
    emp.COD_CNPJ_BASICO as COD_EMPRESA,
    m.COD_MUNICIPIO as COD_MUNICIPIO,
    c.COD_CNAE as COD_CNAE,
    cal.DAT_INICIO_ATIVIDADE as DAT_INICIO_ATIVIDADE,
    COUNT(DISTINCT e.COD_CNPJ) as QTD_ESTABELECIMENTOS
from dim_estabelecimento as e
left join dim_municipio as m on e.COD_MUNICIPIO = m.COD_MUNICIPIO
left join dim_empresa as emp on e.COD_EMPRESA = emp.COD_CNPJ_BASICO
left join dim_cnae as c on e.COD_CNAE_PRINCIPAL = c.COD_CNAE
left join dim_calendario as cal on e.DAT_INICIO_ATIVIDADE = cal.DAT_INICIO_ATIVIDADE
group by 1, 2, 3, 411