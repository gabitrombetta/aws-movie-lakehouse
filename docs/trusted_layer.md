# Camada Trusted

Este documento descreve o processamento dos dados da camada Raw para a camada Trusted do Data Lake, utilizando Apache Spark via AWS Glue.

Nesta etapa, os dados provenientes dos arquivos CSV e da API do TMDb foram processados, padronizados e convertidos para o formato Parquet, otimizando armazenamento e performance de consulta.

# Objetivos da Camada Trusted

A camada Trusted foi responsável por:

- Padronização dos dados
- Conversão de formatos
- Estruturação dos datasets
- Preparação para consumo analítico
- Organização dos dados no Amazon S3

Os dados processados nesta etapa serviram como base para a camada Refined do pipeline analítico.

# Processamento dos Dados CSV

Os dados provenientes dos arquivos CSV foram processados utilizando AWS Glue e Apache Spark.

O pipeline executou:

- Leitura dos arquivos CSV da camada Raw
- Transformação dos dados
- Conversão para o formato Parquet
- Escrita dos dados tratados no Amazon S3

## Tecnologias Utilizadas

- AWS Glue
- Apache Spark
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena

# Catálogo de Dados

Após o processamento, foram criados Crawlers no AWS Glue para catalogação automática dos dados armazenados no S3.

Os Crawlers foram responsáveis por:

- Inferência de schema
- Criação de tabelas no Glue Data Catalog
- Disponibilização dos dados para consulta no Amazon Athena

# Consultas Analíticas

Com os dados catalogados, foram executadas consultas no Amazon Athena para validação e exploração inicial dos datasets processados.

As consultas permitiram verificar:

- Integridade dos dados
- Estrutura das tabelas
- Qualidade do processamento
- Compatibilidade dos schemas

# Processamento dos Dados TMDb

Os dados provenientes da API do TMDb também foram processados utilizando AWS Glue e Apache Spark.

O pipeline executou:

- Leitura dos arquivos JSON armazenados no S3
- Padronização dos dados
- Conversão para Parquet
- Escrita na camada Trusted

# Padronização dos Dados

Para garantir consistência entre os datasets processados, foram realizadas etapas de padronização e organização das tabelas no AWS Glue Data Catalog.

Os dados finais foram separados em conjuntos distintos:

- Dados provenientes dos arquivos CSV
- Dados provenientes da API do TMDb

Essa organização facilitou o consumo analítico nas próximas etapas do pipeline.

# Resultado do Processamento

Ao final da camada Trusted:

- Os dados estavam armazenados em formato Parquet
- Os datasets estavam catalogados no Glue Data Catalog
- As tabelas estavam disponíveis para consulta via Athena
- Os dados estavam preparados para modelagem analítica na camada Refined