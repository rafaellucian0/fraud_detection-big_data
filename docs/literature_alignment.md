# Alinhamento com a literatura

A abordagem FraudX AI foi adaptada para respeitar estritamente o Spark MLlib. O XGBoost foi substituído por `GBTClassifier`, enquanto Random Forest foi mantido com `RandomForestClassifier`.

SHAP foi substituído por recursos compatíveis com MLlib, como `featureImportances` e métricas interpretáveis por threshold. O desbalanceamento de classes é tratado com `weightCol`, não com SMOTE externo.

O threshold tuning foi mantido porque detecção de fraude depende de trade-offs entre recall e precision. O ensemble por média de probabilidades também foi mantido:

```text
fraud_score = (rf_fraud_probability + gbt_fraud_probability) / 2
```

O dataset ULB foi escolhido por ser real, amplamente usado, desbalanceado e compatível com pesquisas de fraude em cartão.

A arquitetura lakehouse segue Bronze/Silver/Gold. Kafka representa o fluxo de transações, Spark Structured Streaming processa micro-batches em near-real-time, Iceberg + Hive Metastore + HDFS fornece tabelas ACID, evolução de schema e gerenciamento de metadados. Airflow orquestra triggers e dependências, mas não substitui a engine streaming.

A inferência ocorre no fluxo Silver/Streaming antes da Gold. A Gold serve BI, histórico, KPIs, alertas e análises OLAP.

