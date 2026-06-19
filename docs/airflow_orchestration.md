# Airflow

Airflow atua como orquestrador de triggers:

- inicialização de namespaces e tabelas Iceberg
- validação do dataset
- ingestão simulada
- submissão de jobs Spark batch
- início ou reinício de jobs Structured Streaming
- geração de Gold
- geração de métricas de qualidade

Airflow não é engine de streaming. Kafka e Spark Structured Streaming são serviços contínuos; as DAGs apenas disparam ou organizam sua execução.

