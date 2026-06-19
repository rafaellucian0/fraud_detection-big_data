# PowerBI

A forma padrão de integração é exportar Gold para CSV:

```text
data/output/powerbi/fraud_scores
data/output/powerbi/fraud_kpis_hourly
data/output/powerbi/fraud_kpis_daily
```

Consultas sugeridas:

- total de transações por período
- quantidade de fraudes previstas
- taxa de fraude
- valor total suspeito
- distribuição de `fraud_score`
- top horários com maior risco
- métricas do modelo
- volume por camada
- qualidade dos dados

Conexão JDBC/ODBC pode ser adicionada com Spark Thrift Server ou HiveServer2.

