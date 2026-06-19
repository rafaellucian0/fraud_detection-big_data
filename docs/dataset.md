# Dataset

Use o European Credit Card / ULB Credit Card Fraud Detection Dataset.

O arquivo esperado é:

```text
data/input/creditcard.csv
```

Colunas obrigatórias:

- `Time`
- `V1` a `V28`
- `Amount`
- `Class`

`Class = 1` representa fraude e `Class = 0` representa transação legítima. O label é mantido para treino e avaliação offline. Em um cenário real, ele não estaria disponível no momento do scoring.

