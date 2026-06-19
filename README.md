# Fraud Lakehouse MLlib

Arquitetura lakehouse local para detecção de fraudes em transações financeiras em near-real-time com Apache Spark, Spark Structured Streaming, Kafka, Apache Iceberg, HDFS, Hive Metastore, Airflow e Spark MLlib.

Este repositório foi preparado para um experimento acadêmico sobre uma pipeline lakehouse local para fraude financeira. A inferência acontece no fluxo Spark/Kafka após a camada Silver e antes da camada Gold.

## Arquitetura

```text
data/input/creditcard.csv
  -> producer Python
  -> Kafka topic transactions.raw
  -> Spark Structured Streaming
  -> Bronze Iceberg/HDFS: local.bronze.transactions_raw
  -> Silver cleaning/schema/features: local.silver.transactions_clean
  -> Spark MLlib batch training: weighted RandomForest + weighted GBT
  -> ensemble fraud_score + threshold tuning
  -> Spark Structured Streaming scoring
  -> Gold Iceberg/HDFS: scores, alerts, KPIs, metrics and data quality
  -> PowerBI via data/output/powerbi CSV exports
```

## Stack

- Spark 3.5.1
- Spark MLlib
- Spark Structured Streaming
- Kafka via Confluent Platform 7.6.1
- Apache Iceberg 1.5.2
- Hadoop/HDFS 3.2.1
- Hive Metastore 3.1.3
- PostgreSQL 11 para Hive Metastore
- PostgreSQL 15.7 para Airflow
- Airflow 2.9.2
- PowerBI por exportação CSV local

## Dataset

O dataset esperado é o European Credit Card / ULB Credit Card Fraud Detection Dataset.

O arquivo já deve estar em:

```text
data/input/creditcard.csv
```

Colunas esperadas:

```text
Time,V1,V2,...,V28,Amount,Class
```

`Class` é preservada apenas para treinamento e avaliação offline. Em um cenário real, esse label não estaria disponível no momento da inferência.

## Pré-requisitos

- Docker Desktop aberto e com engine Linux rodando.
- Docker Compose v2.
- Pelo menos 8 GB de RAM livres para Docker.
- `make` opcional. No Windows, você pode usar os comandos `docker compose ...` mostrados abaixo.

Se o Docker não estiver aberto, comandos como `docker compose build` falham com mensagem sobre `dockerDesktopLinuxEngine`.

## Subir o ambiente

Com `make`:

```bash
make up
```

Sem `make`:

```bash
docker compose up -d --build
```

O primeiro build cria duas imagens locais:

- `fraud-lakehouse-spark:3.5.1`, com dependências Python do projeto.
- `fraud-lakehouse-airflow:2.9.2`, com cliente Docker para disparar jobs via DAGs.

## Serviços e portas

| Serviço | URL/porta |
| --- | --- |
| Spark Master | http://localhost:8080 |
| HDFS NameNode | http://localhost:9870 |
| Airflow | http://localhost:8088 |
| Kafka UI | http://localhost:8089 |
| Kafka externo | localhost:29092 |
| Hive Metastore | localhost:9083 |

Airflow usa usuário `airflow` e senha `airflow`.

## Inicialização

Crie tabelas Iceberg, namespaces e diretórios HDFS:

```bash
make init
```

Sem `make`:

```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 --conf spark.executorEnv.PYTHONPATH=/opt/project/src --conf spark.driverEnv.PYTHONPATH=/opt/project/src /opt/project/jobs/init/create_iceberg_tables.py
```

Crie os tópicos Kafka:

```bash
make create-topics
```

Sem `make`:

```bash
docker compose run --rm kafka-init
```

Valide o dataset:

```bash
make validate-dataset
```

## Fluxo experimental recomendado

1. Subir containers:

```bash
make up
```

2. Inicializar lakehouse:

```bash
make init
make create-topics
make validate-dataset
```

3. Em um terminal, iniciar Bronze streaming:

```bash
make bronze-stream
```

4. Em outro terminal, publicar uma amostra no Kafka:

```bash
make ingest-sample
```

5. Após alguns micro-batches, gerar Silver:

```bash
make silver
```

6. Treinar modelos MLlib:

```bash
make train
```

7. Iniciar scoring streaming:

```bash
make score-stream
```

8. Gerar Gold e métricas de qualidade:

```bash
make gold
make quality
```

Jobs de streaming ficam em execução contínua. Pare com `Ctrl+C` no terminal correspondente.

## Machine Learning

A abordagem adapta o FraudX AI para Spark MLlib:

- `RandomForestClassifier` com `weightCol`.
- `GBTClassifier` com `weightCol`.
- Pesos de classe para lidar com desbalanceamento.
- Ensemble por média:

```text
fraud_score = (rf_fraud_probability + gbt_fraud_probability) / 2
```

- Threshold tuning em `fraud_score`.
- Política padrão: maximizar recall respeitando piso mínimo de precision; se nenhum threshold respeitar o piso, usar best F1.

Métricas principais:

- recall
- precision
- F1-score

Métricas adicionais:

- AUC-PR
- ROC-AUC
- TP, FP, TN, FN

## Restrições científicas

O treinamento principal usa apenas Spark MLlib.

Não são usados:

- XGBoost
- LightGBM
- CatBoost
- SHAP
- SMOTE externo
- TensorFlow
- PyTorch
- scikit-learn como treinamento principal
- Delta Lake
- Hudi

## Airflow

As DAGs em `dags/` disparam os mesmos jobs Spark do fluxo manual. Elas usam `docker exec` para executar comandos dentro do container `fraud-spark-master`.

Airflow é usado para orquestração e triggers. Ele não é a engine contínua do streaming. Kafka e Spark Structured Streaming continuam sendo os componentes responsáveis pelo fluxo near-real-time.

DAGs disponíveis:

- `dag_00_init_lakehouse`
- `dag_01_ingest_to_kafka`
- `dag_02_bronze_streaming`
- `dag_03_silver_processing`
- `dag_04_train_fraud_model`
- `dag_05_score_streaming`
- `dag_06_gold_aggregations`
- `dag_07_data_quality_metrics`

## PowerBI

Gere exports:

```bash
make gold
```

Arquivos:

```text
data/output/powerbi/fraud_scores
data/output/powerbi/fraud_kpis_hourly
data/output/powerbi/fraud_kpis_daily
```

No PowerBI, use a opção de pasta/CSV e carregue os arquivos exportados. As consultas de referência estão em `sql/sample_queries.sql`.

## Testes

Testes locais, se Python e pytest estiverem instalados:

```bash
make test
```

Testes dentro do container Spark:

```bash
make test-docker
```

Sem `make`:

```bash
docker compose exec spark-master bash -lc "cd /opt/project && PYTHONPATH=/opt/project/src:/opt/project pytest -q"
```

## Comandos úteis

```bash
make ps
make logs
make restart
make down
make clean
```

`make clean` remove volumes Docker. Use apenas quando quiser apagar HDFS, metastore, Airflow DB e estado local dos containers.

## Troubleshooting

Se o build falhar por Docker daemon indisponível, abra o Docker Desktop e aguarde a engine Linux iniciar.

Se o Spark falhar baixando jars Maven, verifique a conexão de rede no primeiro `spark-submit`. Os pacotes Iceberg, Kafka e PostgreSQL são resolvidos na primeira execução.

Se Airflow não conseguir executar `docker exec`, confirme que o Docker socket está montado e que o Docker Desktop permite acesso de containers ao daemon.

Se `make` não existir no Windows, use os comandos `docker compose` equivalentes documentados neste README.

## Estrutura

```text
configs/              Configurações Spark, Hive, Hadoop, Kafka e app
dags/                 DAGs Airflow
jobs/                 Jobs Spark, producer Kafka e rotinas de qualidade
src/fraud_lakehouse/  Código reutilizável
sql/                  DDLs Iceberg e consultas
docs/                 Documentação acadêmica
tests/                Testes unitários
data/input/           creditcard.csv
data/output/powerbi/  Exports para BI
models/               Metadados locais do último treino
```
