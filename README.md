# Projeto RFB

Pipeline de engenharia de dados para ingestão, transformação e disponibilização dos dados cadastrais públicos de empresas da Receita Federal do Brasil (CNPJ), seguindo a arquitetura Medallion.

## Objetivo

Processar os dados abertos do CNPJ da RFB em um formato estruturado e analítico, passando pelas camadas Bronze, Silver e Gold com rastreabilidade e qualidade de dados em cada etapa.

## Stack

- **Databricks Community Edition** — plataforma de processamento distribuído
- **Delta Lake** — formato de armazenamento nas camadas Bronze, Silver e Gold
- **dbt** — transformações SQL a partir da camada Bronze
- **Python / PySpark** — ingestão e carga inicial
- **Databricks SDK** — upload de arquivos para Volumes
- **Scrapy** — crawler para coleta dos arquivos no portal da RFB
- **sidrapy** — fonte de dados do PIB (IBGE/SIDRA)
- **Astro** — Gerenciador do Airflow

## Arquitetura

```plaintext
Fonte (RFB)
    │
    ▼
Staging (Volume)        ← arquivos .zip brutos da RFB
    │
    ▼
Bronze (Delta Table)    ← CSV extraído, sem header, tudo como string
    │
    ▼
Silver (dbt)            ← schema aplicado, tipos corretos, colunas nomeadas
    │
    ▼
Gold (dbt)              ← agregações e visões analíticas
```

## Estrutura do Projeto

```plaintext
rfb/
├── dags/
│   ├── ingestion/
│   └── transformation/
├── rfb_crawler/
│   ├── spiders/
│   │   └── rfb_spider.py
│   ├── pipelines.py
│   └── settings.py
├── src/
│   ├── ingestion/
│   │   ├── download_rfb_data.py
│   │   ├── send_data_to_remote.py
│   │   └── dto/
│   └── dbt/
│       ├── models/
│       ├── macros/
│       ├── seeds/
│       └── dbt_project.yml
└── tests/
    └── dags/
```

## Fontes de Dados

- **CNPJ (RFB):** [Portal de Dados Abertos da Receita Federal](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9), atualizado mensalmente
- **PIB:** API do IBGE via `sidrapy`

## Como Rodar

> **Pré-requisito:** é necessário ter acesso ao Databricks (pode ser a Community Edition) com Unity Catalog habilitado.

### 1. Configurar o Unity Catalog

No Databricks, abra um notebook e execute os comandos abaixo para criar o catálogo e os schemas necessários:

```python
spark.sql("CREATE CATALOG IF NOT EXISTS rfb")
spark.sql("CREATE SCHEMA IF NOT EXISTS rfb.transient")
spark.sql("CREATE SCHEMA IF NOT EXISTS rfb.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS rfb.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS rfb.gold")
```

### 2. Instalar dependências locais

```bash
pip install poetry
poetry install
```

### 3. Configurar variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com as credenciais do Databricks:

```env
DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<your-token>
```

### 4. Executar airflow

```bash
# Inicia uma instancia do airflow, podendo rodar maunalmente as dags ou com agendamento
astro dev start
```

### 5. Executar as transformações com dbt

```bash
cd src/dbt
dbt deps
dbt run
```

As DAGs de ingestão e transformação ficam disponíveis na interface do Airflow em `localhost:8080`.
