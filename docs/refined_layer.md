# Camada Refined (Modelagem Dimensional)

Este documento descreve a camada Refined do pipeline de dados desenvolvido na AWS, responsável pela modelagem dimensional e consolidação dos dados provenientes da camada Trusted.

Nesta etapa, os dados foram estruturados em um modelo dimensional do tipo estrela (Star Schema), com o objetivo de otimizar consultas analíticas e suportar a criação de dashboards.

# Objetivo da Camada Refined

A camada Refined tem como principal objetivo transformar os dados da camada Trusted em um modelo analítico estruturado, facilitando consultas e análises de negócio.

As principais responsabilidades desta camada são:

- Modelagem dimensional dos dados
- Criação de tabela fato e dimensões
- Consolidação de dados de múltiplas fontes (CSV e API TMDb)
- Otimização para consumo analítico
- Preparação para visualização no Amazon QuickSight

# Modelagem Dimensional

A modelagem foi realizada utilizando o conceito de Star Schema, estruturada da seguinte forma:

## Tabela Fato

- **Fato Avaliações de Filmes**
  - Contém métricas de avaliação e popularidade dos filmes
  - Relaciona-se com as dimensões de filmes, elenco e data

## Tabelas Dimensão

- **Dimensão Filmes**
  - Informações detalhadas dos filmes (título, gênero, palavras-chave)

- **Dimensão Elenco**
  - Informações sobre atores e atrizes associados aos filmes

- **Dimensão Data**
  - Permite análise temporal baseada no ano de lançamento

# Processamento dos Dados

O processamento foi realizado utilizando:

- AWS Glue
- Apache Spark
- PySpark DataFrame API

O pipeline executou as seguintes etapas:

- Leitura dos dados da camada Trusted (CSV e TMDb)
- Filtragem dos gêneros Drama e Romance
- Conversão e padronização de tipos de dados
- Enriquecimento dos datasets
- Criação de dimensões e tabela fato
- Remoção de duplicidades
- Persistência dos dados no Amazon S3 em formato Parquet

# Lógica de Modelagem

O processo de modelagem seguiu as seguintes regras:

- Unificação de dados provenientes de múltiplas fontes
- Normalização de atributos entre datasets CSV e API TMDb
- Criação de identificadores únicos para entidades
- Relacionamento entre fato e dimensões por chaves lógicas
- Garantia de consistência entre registros

# Arquitetura de Processamento

O pipeline da camada Refined foi executado via AWS Glue Job, utilizando Spark distribuído para processamento em larga escala.

Fluxo do processamento:

- Leitura dos dados Trusted
- Transformações com Spark
- Criação de dimensões
- Criação da tabela fato
- Escrita no Amazon S3

# Persistência dos Dados

Os dados processados foram armazenados no Amazon S3 no formato Parquet, organizados em:

- `dim_filmes/`
- `dim_elenco/`
- `dim_data/`
- `fato_avaliacoes_filme/`

Essa estrutura garante otimização para consultas analíticas.

# Catálogo de Dados e Consulta

Após o processamento, foi utilizado o AWS Glue Crawler para catalogação automática das tabelas.

As tabelas foram disponibilizadas no AWS Glue Data Catalog e consultadas via Amazon Athena.

# Validação dos Dados

Foram realizadas consultas no Athena para validação da camada Refined, verificando:

- Integridade das tabelas
- Consistência entre dimensões e fato
- Qualidade da modelagem
- Distribuição dos dados

# Resultado da Camada Refined

Ao final do processamento:

- Os dados foram estruturados em modelo dimensional
- O Star Schema foi implementado com sucesso
- As tabelas foram catalogadas no Glue Data Catalog
- Os dados foram disponibilizados para consumo analítico no Athena e QuickSight