# Arquitetura

O projeto usa uma arquitetura lakehouse local com separação Bronze/Silver/Gold.

Bronze preserva os eventos Kafka quase crus, convertendo JSON para tabela Iceberg/Parquet e adicionando metadados técnicos como tópico, partição, offset, timestamp de ingestão e identificador do evento.

Silver aplica schema enforcement, tipos numéricos, tratamento de nulos, criação de `transaction_id`, `event_timestamp`, `label`, `class_weight` e `feature_array`.

O treinamento batch lê Silver e salva artefatos MLlib em HDFS. O scoring em streaming ocorre após Silver e antes de gravar Gold.

Gold armazena scores, alertas, KPIs horários/diários, métricas de modelo e métricas de qualidade. PowerBI consome os exports locais em `data/output/powerbi`.

