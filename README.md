# Projeto RFB

Projeto de engenharia de dados e analytics com os dados públicos da Receita Federal do Brasil (CNPJ).

## Objetivo

Ingerir, transformar e disponibilizar os dados cadastrais de empresas da RFB em um formato estruturado e analítico, seguindo a arquitetura Medallion.

## Stack

- **Databricks Community Edition** — plataforma de processamento
- **Delta Lake** — formato de armazenamento nas camadas Bronze, Silver e Gold
- **DBT** — transformações SQL a partir da Bronze
- **Python / PySpark** — ingestão e carga inicial
- **Databricks SDK** — upload de arquivos para Volumes

## Arquitetura

```shell
Fonte (RFB)
    │
    ▼
Staging (Volume)        ← arquivos .zip brutos da RFB
    │
    ▼
Bronze (Delta Table)    ← CSV extraído, sem header, tudo como string
    │
    ▼
Silver (DBT)            ← schema aplicado, tipos corretos, colunas nomeadas
    │
    ▼
Gold (DBT)              ← agregações e visões analíticas
```

## Estrutura do Projeto

```shell
rfb/
├── airflow_settings.yaml
├── dags
│   └── ingestion
│       └── ingst_rfb_data.py
├── Dockerfile
├── include
├── jobs
│   └── readme.md
├── packages.txt
├── plugins
├── poetry.lock
├── pyproject.toml
├── README.md
├── requirements.txt
├── rfb_crawler
│   ├── __init__.py
│   ├── items.py
│   ├── middlewares.py
│   ├── pipelines.py
│   ├── settings.py
│   └── spiders
│       ├── __init__.py
│       └── rfb_spider.py
├── scrapy.cfg
├── src
│   ├── ingestion
│   │   ├── download_rfb_data.py
│   │   ├── dto
│   │   └── send_data_to_remote.py
│   └── transformation
│       ├── dbt_internal_packages
│       ├── dbt_packages
│       ├── dbt_project.yml
│       ├── logs
│       ├── macros
│       ├── models
│       ├── package-lock.yml
│       ├── packages.yml
│       ├── README.md
│       ├── seeds
│       └── target
└── tests
    ├── dags
    │   └── test_dag_example.py
    └── __init__.py

```

## Dados

Os dados são provenientes do portal de [Dados Abertos da Receita Federal](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9), atualizados mensalmente.
