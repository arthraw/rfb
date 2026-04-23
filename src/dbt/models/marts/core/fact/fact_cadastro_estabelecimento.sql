with
dim_municipio as (
    select
        COD_MUNICIPIO
    from {{ ref('dim_municipio') }}
),
dim_cnae as (
    select
        COD_CNAE
    from {{ ref('dim_cnae') }}
),
dim_empresa as (
    select
        e.COD_EMPRESA as COD_EMPRESA
    from {{ ref('dim_empresa') }} e
),
dim_estabelecimento as (
    select
        COD_CNPJ,
        COD_EMPRESA,
        COD_MUNICIPIO,
        COD_CNAE,
        DAT_INICIO_ATIVIDADE
    from {{ ref('dim_estabelecimento') }}
),
dim_calendario as (
    select
        DAT_INICIO_ATIVIDADE
    from {{ ref('dim_calendario') }}
)
select
    e.COD_EMPRESA as COD_EMPRESA,
    e.COD_MUNICIPIO as COD_MUNICIPIO,
    e.COD_CNAE as COD_CNAE,
    e.DAT_INICIO_ATIVIDADE as DAT_INICIO_ATIVIDADE,
    COUNT(DISTINCT e.COD_CNPJ) as QTD_ESTABELECIMENTOS
from dim_estabelecimento as e
inner join dim_municipio as m on e.COD_MUNICIPIO = m.COD_MUNICIPIO
inner join dim_empresa as emp on e.COD_EMPRESA = emp.COD_EMPRESA
inner join dim_cnae as c on e.COD_CNAE = c.COD_CNAE
inner join dim_calendario as cal on e.DAT_INICIO_ATIVIDADE = cal.DAT_INICIO_ATIVIDADE
group by 1, 2, 3, 4