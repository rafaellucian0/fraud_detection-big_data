# Metodologia Experimental

O experimento usa o dataset ULB com `Time`, `V1` a `V28`, `Amount` e label `Class`.

## Protocolo temporal

`configs/app/experiment.yaml` define uma divisao cronologica 60/20/20 pela coluna `Time`:

- treino: primeiros 60% do fluxo;
- validacao: 20% seguintes, usados para hiperparametros e limiar;
- teste: 20% finais, mantidos isolados ate a selecao do candidato.

O job executa cinco sementes (`42`, `52`, `62`, `72`, `82`) por candidato e reporta media e desvio-padrao. PR-AUC e a metrica primaria de selecao; ROC-AUC, precision, recall, F1 e a matriz de confusao sao reportados como metricas complementares. Esse desenho evita que o teste influencie o ajuste e respeita a ordem temporal da fonte.

## Modelos e desbalanceamento

Sao treinados somente modelos Spark MLlib. Cada candidato combina um `RandomForestClassifier` ponderado e um `GBTClassifier` ponderado. O score do ensemble e a media das probabilidades de fraude:

```text
fraud_score = (rf_fraud_probability + gbt_fraud_probability) / 2
```

Os pesos sao calculados apenas no bloco de treino:

```text
weight_0 = total / (2 * legit_count)
weight_1 = total / (2 * fraud_count)
```

O limiar e escolhido no conjunto de validacao por busca em grade, priorizando o maior recall com piso de precision configuravel; `best_f1` permanece como fallback quando nenhum limiar atende ao piso.

## Artefatos

O job `jobs/training/train_fraud_model.py` grava cada run em HDFS e registra resultados em:

- `local.gold.experiment_metrics`: run, semente, split, limiar, AUCs e matriz de confusao;
- `local.gold.experiment_summary`: media e desvio-padrao por candidato e split;
- `models/latest_metadata.json`: configuracao vencedora e modelo de implantacao.

Depois da avaliacao, o modelo de implantacao e treinado em treino mais validacao. `jobs/scoring/score_silver_batch.py` materializa scores e alertas para todo o conteudo Silver; o job de streaming preserva a mesma logica para eventos novos.
