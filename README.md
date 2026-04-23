# Projeto RFB

Pipeline de engenharia de dados para ingestão, transformação e disponibilização dos dados cadastrais públicos de empresas da Receita Federal do Brasil (CNPJ), seguindo a arquitetura Medallion.

## Objetivo

Processar os dados abertos do CNPJ da RFB em um formato estruturado e analítico, passando pelas camadas Bronze, Silver e Gold com rastreabilidade e qualidade de dados em cada etapa.

## Desafio

O objetivo principal deste projeto foi transformar o caos de dados brutos da Receita Federal e do IBGE em uma arquitetura analítica organizada, capaz de responder a perguntas estratégicas de expansão de mercado.

### A Problemática

Imagine uma empresa que deseja expandir sua atuação no Brasil. Olhar apenas para a contagem total de CNPJs por cidade é um erro estratégico comum, pois não diferencia o potencial econômico real das regiões. O desafio consistiu em:

- Tratamento de Volume: Processar milhões de registros da Receita Federal (estabelecimentos e empresas) garantindo a integridade dos dados (como o tratamento de zeros à esquerda em CNPJs e CEPs).

- Cruzamento de Fontes Distintas: Unificar dados cadastrais (RFB) com indicadores macroeconômicos (PIB Municipal - IBGE).

- Modelagem Star Schema: Estruturar as camadas de dados (Staging, Intermediate e Marts) para que um analista de negócios possa extrair insights sem necessidade de Joins complexos ou limpeza manual.

### O Valor de Negócio

Com a modelagem final (fct_estabelecimentos cruzada com dim_municipio), o projeto permite identificar:

- Municípios com PIB per capita alto mas baixa densidade de empresas de grande porte.

- Sazonalidade e ritmo de abertura de novas empresas nos últimos anos.

- Perfil de saúde econômica por região, permitindo que o time de marketing direcione investimentos para onde há maior capital circulante.

## Stack

- **Databricks Community Edition** — plataforma de processamento distribuído
- **Delta Lake** — formato de armazenamento nas camadas Bronze, Silver e Gold
- **dbt** — transformações SQL a partir da camada Bronze
- **Python / PySpark** — ingestão e carga inicial
- **Databricks SDK** — upload de arquivos para Volumes
- **Scrapy** — crawler para coleta dos arquivos no portal da RFB
- **sidrapy** — fonte de dados do PIB (IBGE/SIDRA)
- **Astro** — Gerenciador do Airflow
- **GitHub Actions** — CI/CD Workflows
- **Great Expectatio** — Testes de qualidade dos dados
- **Astronomer Cosmos** — Gerenciamento do DBT nas dags do airflow

## Arquitetura

```plaintext
Fonte (RFB)
    │
    ▼
Staging (Volume)        ← arquivos .csv brutos da RFB e IBGE
    │
    ▼
Bronze (Delta Table)    ← dados sem tratamento com schema/colunas basicas aplicadas (apenas para organizar em tabelas)
    │
    ▼
Silver (dbt)            ← schema aplicado, tipos corretos, colunas nomeadas
    │
    ▼
Gold (dbt)              ← agregações e visões analíticas
```

## Modelagem de Dados - Gold Layer

![Modelagem das tabelas (Gold Layer)](/imgs/modelagem.png)

## Estrutura do Projeto

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

![Dags de ingestao](/imgs/dag_ingestao.png)

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

### 5. Criar Variables do Airflow

Crie as variaveis do databricks para as dags conseguirem conectar com fontes externas. (Ex: Databricks)

![Tela do airflow com as variaveis](/imgs/variables_airflow.png)

### 6. Executar as transformações com dbt OU Executar as dags direto na UI do airflow

```bash
cd src/dbt
dbt deps
dbt run
```

> Obs: Caso queira testar todo o fluxo pode rodar direto as dags no airflow.

As DAGs de ingestão e transformação ficam disponíveis na interface do Airflow em `localhost:8080`.
