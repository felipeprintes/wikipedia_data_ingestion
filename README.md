# Projeto ingestão de dados Wikipedia

## Objetivo do projeto
O objetivo deste projeto em específico foi realizar a ingestão de dados da Wikipedia, extraindo métricas e estatísticas de visualizações de páginas e outras informações relevantes usando a API da Wikimedia.
Este projeto foi executado localmente em uma escala bem pequena, extraindo informações apenas de um título da categoria de games para Android: Angry Birds.

## Estrutura do projeto
### Past airflow
- dags: Contém o arquivo pipeline.py, responsável por montar o pipeline airflow e orquestrar o pipeline de ponta a ponta

- jobs: pasta que contém a pasta "app", pasta essa que contém toda a lógica para aplicação. Na pasta app, se econtrar os arquivos:
    - connectors.py: contém classes e métodos facilitadores para requisição a api e ingestão no filesystem
    - endpoints.py: contém as regras de negócio para ingestão
    - cleaning_job.py: Contém job pyspark responsável por ler da landing para escrever na cleaning em parquet
    - cleaning_to_postgres.py: Contém o código pyspark responsável por ler os arquivos parquet da cleaning para escrever no postgres que alimentará os painéis metabase.
- #### OBS: Idealmente esses dados seriam trabalhados no S3, porém para facilitar, foi escrito localmente.

- Lake: Contém as pastas Landing e Cleaning
    - Landing: Arquivos brutos com dados vindos da api sem nenhum tratamento
    - Cleaning: Algumas transformações, padronizações, tratamentos de datas e escritos em parquet


### docker-compose
Responsável por subir toda infra localmente necessária para executar o pipeline ponta a ponta. Para esse projeto, foi utilizado: 
- Apache Airflow
- Metabase
- Pyspark
- Python
- Postgres

### Como Rodar
> docker-compose up --build

- localhost:8080 [airflow]
- localhost:3000 [metabase ]
    

