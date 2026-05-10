# Camada de Ingestão de Dados

Este documento descreve a camada de ingestão do pipeline de dados desenvolvido na AWS, responsável pela coleta e armazenamento inicial dos dados em um Data Lake.

Os dados utilizados foram obtidos a partir de arquivos CSV e da API do TMDb, sendo armazenados no Amazon S3 para posterior processamento nas camadas Trusted e Refined.

A análise possui foco nos gêneros Drama e Romance, buscando identificar padrões, tendências e métricas relevantes ao longo do tempo.

# Questões Analíticas

A análise foi estruturada para investigar padrões e tendências relacionados aos gêneros Drama e Romance.

As principais questões analíticas definidas para o projeto foram:

1. Como é a distribuição dos gêneros Romance e Drama?
2. Entre os filmes do gênero Drama, qual tema é mais retratado?
3. Como evoluiu a presença feminina nos gêneros Drama e Romance, por década, desde 1950?
4. Como a popularidade dos filmes de Drama e Romance evoluiu ao longo das décadas?
5. Como as notas dos filmes de Drama e Romance evoluíram na última década?
6. Qual é a proporção entre artistas homens e mulheres nas produções analisadas?

# Arquitetura da Ingestão

A camada de ingestão foi dividida em dois fluxos principais:

- Ingestão de arquivos CSV
- Ingestão de dados via API do TMDb

Todos os dados foram armazenados no Amazon S3, compondo a camada Raw do Data Lake.

# Ingestão de Arquivos CSV

A ingestão inicial dos dados foi realizada a partir de arquivos CSV contendo informações sobre filmes e séries.

O processo foi implementado em Python utilizando a biblioteca `boto3`, responsável pela integração com o Amazon S3 e envio dos arquivos para a camada Raw.

## Containerização com Docker

O processo de ingestão foi containerizado utilizando Docker, garantindo:

- Portabilidade
- Padronização do ambiente
- Facilidade de execução

O container foi configurado com:

- Imagem base `python:3`
- Diretório de trabalho `/app`
- Instalação automática das dependências via `requirements.txt`
- Execução automatizada do script de upload

## Build da imagem

```bash
docker build -t upload-s3 .
```

## Execução do container

```bash
docker run --env-file .env -v "$(pwd)/data:/app/data" upload-s3
```

### Parâmetros utilizados

- `--env-file .env`  
  Carrega as variáveis de ambiente necessárias para autenticação e acesso aos serviços AWS.

- `-v "$(pwd)/data:/app/data"`  
  Realiza o mapeamento da pasta local contendo os arquivos CSV para o container.

- `upload-s3`  
  Define a imagem Docker executada.

# Ingestão via API do TMDb

A segunda etapa da ingestão foi responsável pela coleta de dados complementares através da API do TMDb utilizando AWS Lambda.

Foi desenvolvido um script Python responsável por:

- Consumir a API do TMDb
- Processar os dados retornados
- Armazenar os resultados no Amazon S3

# Configuração do AWS Lambda

Foi criada uma função AWS Lambda para execução serverless do processo de ingestão.

Para inclusão das dependências externas, foi utilizada uma Lambda Layer contendo as bibliotecas necessárias para execução do pipeline.

O código responsável pelo consumo da API foi implantado diretamente na função Lambda.

# Permissões e Segurança

Foi adicionada uma política IAM permitindo que a função Lambda realizasse operações de escrita no bucket S3.

Essa configuração garantiu acesso controlado aos recursos utilizados durante o processo de ingestão.

# Resultado da Ingestão

Após a execução dos processos de ingestão:

- Os arquivos CSV foram armazenados corretamente na camada Raw
- Os dados provenientes da API do TMDb foram enriquecidos e persistidos no Amazon S3
- A estrutura inicial do Data Lake foi consolidada para as próximas etapas de processamento