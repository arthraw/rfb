with source as (
    select * from {{ source('raw', 'raw_estabelecimento') }}
),
estabelecimentos as (
    select
        concat(cnpj_basico, cnpj_ordem, cnpj_dv)                    as COD_CNPJ,
        case cast(identificador_matriz_filial as int)
            when 1 then 'MATRIZ'
            when 2 then 'FILIAL'
            else 'NÃO INFORMADO'
        end                                                         as DSC_MATRIZ_FILIAL,
        coalesce(upper(nome_fantasia), 'NÃO INFORMADO')             as NOM_FANTASIA,
        case cast(situacao_cadastral as int)
            when 1 then 'NULA'
            when 2 then 'ATIVA'
            when 3 then 'SUSPENSA'
            when 4 then 'INAPTA'
            when 8 then 'BAIXADA'
            else 'NÃO INFORMADO'
        end                                                         as DSC_SITUACAO_CADASTRAL,
        try_to_date(data_situacao_cadastral, 'yyyyMMdd')            as DAT_SITUACAO_CADASTRAL,
        motivo_situacao_cadastral                                   as DSC_MOTIVO_SITUACAO_CADASTRAL,
        nome_cidade_exterior                                        as NOM_CIDADE_EXTERIOR,
        pais                                                        as DSC_PAIS,
        try_to_date(data_inicio_atividade, 'yyyyMMdd')              as DAT_INICIO_ATIVIDADE,
        cnae_principal                                              as DSC_CNAE_PRINCIPAL,
        cnae_secundario                                             as DSC_CNAE_SECUNDARIO,
        upper(tipo_logradouro)                                      as TPO_LOGRADOURO,
        upper(logradouro)                                           as DSC_LOGRADOURO,
        coalesce(upper(numero), 'S/N')                              as DSC_NUM_FACHADA,
        upper(complemento)                                          as DSC_COMPLEMENTO,
        upper(bairro)                                               as DSC_BAIRRO,
        upper(cep)                                                  as DSC_CEP,
        upper(uf)                                                   as DSC_UF,
        upper(municipio)                                            as DSC_MUNICIPIO,
        case
            when ddd1 is not null and telefone1 is not null
            then concat(ddd1, telefone1)
        end                                                         as DSC_TELEFONE1,
        case
            when ddd2 is not null and telefone2 is not null
            then concat(ddd2, telefone2)
        end                                                         as DSC_TELEFONE2,
        case
            when ddd_fax is not null and fax is not null
            then concat(ddd_fax, fax)
        end                                                         as DSC_FAX,
        email                                                       as DSC_EMAIL,
        situacao_especial                                           as DSC_SITUACAO_ESPECIAL,
        try_to_date(data_situacao_especial, 'yyyyMMdd')             as DAT_SITUACAO_ESPECIAL
    from source
)
select * from estabelecimentos