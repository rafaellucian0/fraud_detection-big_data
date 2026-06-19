# AGENTS.md

## Contexto

Este repositório implementa uma arquitetura lakehouse local para detecção de fraudes em transações financeiras em near-real-time. O projeto é base experimental para um artigo científico sobre Apache Spark, Spark Structured Streaming, Kafka, Apache Iceberg, HDFS, Hive Metastore, Airflow e Spark MLlib.

## Objetivo científico

Avaliar se uma pipeline lakehouse local consegue ingerir transações via Kafka, preparar dados em camadas Bronze/Silver/Gold, treinar modelos Spark MLlib em dados desbalanceados e aplicar inferência em streaming antes da camada Gold.

## Restrições obrigatórias

- Use somente Spark MLlib para o treinamento principal.
- Não use XGBoost, LightGBM, CatBoost, SHAP, SMOTE externo ou deep learning externo.
- Não substitua Apache Iceberg por Delta Lake ou Hudi.
- Não substitua HDFS por armazenamento local simples como camada principal.
- Não remova Hive Metastore.
- Não mova a inferência para a camada Gold; o scoring ocorre após Silver e antes da Gold.
- Não use Airflow como controlador contínuo de Kafka ou Spark Structured Streaming. Airflow dispara jobs e organiza dependências.
- Não use pandas como engine principal de processamento.

## Convenções de código

- Jobs Spark ficam em `jobs/`.
- Funções reutilizáveis ficam em `src/fraud_lakehouse/`.
- Funções puras devem ser testadas em `tests/`.
- Prefira logs claros, validações explícitas e erros acionáveis.
- Mantenha configurações em `configs/app/*.yaml`.

## Convenções de tabelas

- Bronze: `local.bronze.*`, dados crus com metadados técnicos.
- Silver: `local.silver.*`, dados limpos, tipados e preparados.
- Gold: `local.gold.*`, KPIs, scores, alertas, métricas e qualidade.
- ML: `local.ml.*`, metadados auxiliares de experimentos quando necessário.

## Como adicionar features

1. Atualize `FEATURE_COLUMNS` em `src/fraud_lakehouse/schemas.py`.
2. Atualize `configs/app/model.yaml`.
3. Ajuste DDLs em `sql/create_silver_tables.sql` se a feature for materializada.
4. Adicione testes para schema e transformação.
5. Documente a mudança em `docs/methodology.md`.

## Como adicionar métricas

1. Implemente funções puras em `src/fraud_lakehouse/metrics.py` quando possível.
2. Integre a métrica nos jobs de treino, Gold ou qualidade.
3. Grave resultados em `local.gold.model_metrics` ou `local.gold.data_quality_metrics`.
4. Inclua consulta em `sql/sample_queries.sql`.

## Reprodutibilidade

- Fixe versões em `docker-compose.yml`, `pyproject.toml` e `requirements.txt`.
- Não baixe automaticamente o dataset ULB; o usuário deve posicionar `data/input/creditcard.csv`.
- Registre `model_version`, threshold, pesos de classe, colunas de features e timestamp de treino.

