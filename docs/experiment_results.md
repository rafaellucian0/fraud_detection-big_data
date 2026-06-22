# Resultados da Execucao Completa

Data da execucao: 21 de junho de 2026.

## Dados e pipeline

- Dataset ULB: 284.807 transacoes e 492 fraudes.
- Eventos publicados em Kafka: 284.807.
- Bronze, Silver e Gold scores: 284.807 registros cada.
- Alertas Gold: 562.
- Metricas de qualidade Gold: 151.
- Testes no container Spark: 12 aprovados.

## Protocolo

- Split temporal: 170.229 treino, 56.732 validacao e 56.765 teste.
- Cortes em `Time`: 120390 e 145229 segundos.
- Runs por candidato: 5, sementes 42, 52, 62, 72 e 82.
- Criterio de selecao: maior PR-AUC media de validacao.
- Candidato vencedor: `compact` (RF: 80 arvores/profundidade 8; GBT: 50 iteracoes/profundidade 3).
- Limiar de implantacao: 0,70.

## Teste Isolado

Media mais desvio-padrao nas cinco sementes do candidato vencedor:

| Metrica | Resultado |
| --- | --- |
| PR-AUC | 0,749825 +- 0,016599 |
| ROC-AUC | 0,978700 +- 0,004384 |
| Precision | 0,861579 +- 0,025262 |
| Recall | 0,735135 +- 0,015408 |
| F1 | 0,793038 +- 0,009232 |

## Materializacao Operacional

O modelo de implantacao foi ajustado em treino mais validacao e aplicado retrospectivamente a toda a Silver. Esse numero nao e uma metrica de generalizacao e nao substitui a tabela de teste acima.

- Verdadeiros positivos: 435.
- Falsos positivos: 127.
- Falsos negativos: 57.
- Precision retrospectiva: 0,7731.
- Recall retrospectivo: 0,8841.
