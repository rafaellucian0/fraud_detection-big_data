# Metodologia

O experimento segue o dataset ULB com features `Time`, `V1` a `V28`, `Amount` e label `Class`.

O desbalanceamento é tratado por pesos de classe:

```text
weight_0 = total / (2 * legit_count)
weight_1 = total / (2 * fraud_count)
```

São treinados Random Forest e GBT do Spark MLlib. A probabilidade de fraude dos dois modelos é combinada por média simples. O threshold final é escolhido por busca em grade, priorizando recall sob piso mínimo de precision e usando best F1 como fallback.

