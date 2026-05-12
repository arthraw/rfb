# Projeto RFB

Pipeline de engenharia de dados para ingestão, transformação e disponibilização dos dados cadastrais públicos de empresas da Receita Federal do Brasil (CNPJ), seguindo a arquitetura Medallion.

## Objetivo

Transformar os dados abertos do CNPJ da RFB em um formato estruturado e analítico, passando pelas camadas Bronze, Silver e Gold com rastreabilidade e qualidade em cada etapa.

## Desafio

O principal objetivo deste projeto foi organizar os dados brutos da Receita Federal e do IBGE em uma arquitetura analítica consistente, capaz de responder a perguntas estratégicas sobre expansão de mercado.

### A problemática

Imagine uma empresa que deseja expandir sua atuação no Brasil. Olhar apenas para a contagem total de CNPJs por cidade é um erro comum, pois não considera o potencial econômico real das regiões. O desafio consistiu em:

- **Tratamento de volume:** processar milhões de registros da Receita Federal (estabelecimentos e empresas) garantindo a integridade dos dados, incluindo o tratamento de zeros à esquerda em CNPJs e CEPs.
- **Cruzamento de fontes distintas:** unificar dados cadastrais da RFB com indicadores macroeconômicos do IBGE.
- **Modelagem em Star Schema:** estruturar as camadas de dados (Staging, Intermediate e Marts) para permitir análises sem necessidade de joins complexos ou limpeza manual.

### Valor de negócio

Com a modelagem final (fact_cadastro_estabelecimento cruzada com dim_municipio), o projeto permite identificar:

- municípios com PIB per capita alto e baixa densidade de empresas de grande porte;
- região com perfil de saúde econômica favorável para direcionar investimentos de marketing e expansão.

## Stack

- **Databricks Community Edition** — plataforma de processamento distribuído
- **Delta Lake** — formato de armazenamento nas camadas Bronze, Silver e Gold
- **dbt** — transformações SQL a partir da camada Bronze
- **Python / PySpark** — ingestão e carga inicial
- **Databricks SDK** — upload de arquivos para volumes
- **Scrapy** — crawler para coleta dos arquivos no portal da RFB
- **sidrapy** — fonte de dados do PIB (IBGE/SIDRA)
- **Astro** — gerenciador do Airflow
- **GitHub Actions** — CI/CD workflows
- **Great Expectations** — testes de qualidade dos dados
- **Astronomer Cosmos** — gerenciamento do dbt nas DAGs do Airflow

## Arquitetura

```plaintext
Fonte (RFB)
    │
    ▼
Staging (Volume)        ← arquivos .csv brutos da RFB e do IBGE
    │
    ▼
Bronze (Delta Table)    ← dados sem tratamento, com schema/colunas básicas aplicadas (apenas para organizar em tabelas)
    │
    ▼
Silver (dbt)            ← schema aplicado, tipos corretos, colunas nomeadas
    │
    ▼
Gold (dbt)              ← agregações e visões analíticas
```

## Modelagem de Dados - Gold Layer

![Modelagem das tabelas (Gold Layer)](/imgs/modelagem.png)

## Estrutura do projeto

```plaintext
rfb/
├── dags/
│   ├── ingestion/
│   └── transformation/
├── jobs/ # Jobs para ingestão dos dados
├── quality/ # Validação com Data Quality
├── rfb_crawler/
│   ├── spiders/
├── src/
│   ├── ingestion/
│   └── dbt/
│       ├── models/
│       ├── macros/
│       ├── seeds/
│       ├── tests/
└── tests/
    └── dags/
```

![DAGs de ingestão](/imgs/dag_ingestao.png)

## Fontes de dados

- **CNPJ (RFB):** [Portal de Dados Abertos da Receita Federal](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9) — atualizado mensalmente
- **PIB:** API do IBGE via `sidrapy`
- **Municípios:** CSV com os códigos de municípios IBGE para cruzar a Receita com o IBGE

## Como rodar

> **Pré-requisito:** ter acesso ao Databricks (pode ser a Community Edition) com Unity Catalog habilitado.

### 1. Configurar o Unity Catalog

No Databricks, abra um notebook e execute:

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

### 4. Executar o Airflow

```bash
# Inicia uma instância do Airflow, permitindo executar as DAGs manualmente ou por agendamento
astro dev start
```

### 5. Criar variáveis no Airflow

Configure as variáveis do Databricks para que as DAGs consigam conectar às fontes externas.

![Tela do Airflow com as variáveis](/imgs/variables_airflow.png)

### 6. Executar transformações com dbt ou usar a UI do Airflow

```bash
cd src/dbt
dbt deps
dbt run
```

> Observação: se quiser testar todo o fluxo, pode executar as DAGs diretamente no Airflow.

As DAGs de ingestão e transformação ficam disponíveis em `localhost:8080`.
