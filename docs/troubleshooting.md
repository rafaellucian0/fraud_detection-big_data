# Troubleshooting

## Dataset não encontrado

Confirme que `data/input/creditcard.csv` existe e contém as colunas esperadas.

## Pacotes Spark/Iceberg não baixam

O Spark baixa dependências Maven na primeira execução. Se o ambiente estiver sem internet, pré-carregue os jars no volume Spark ou ajuste `spark.jars`.

## Airflow não executa comandos make

As DAGs representam a orquestração. Em deployments containerizados, o worker precisa de acesso ao cliente Spark ou ao Docker Compose do host.

## Escrita streaming em Iceberg falha

Use o padrão `foreachBatch`, já aplicado no scoring. Para Bronze, valide a compatibilidade da versão Iceberg/Spark; se necessário, adapte também para `foreachBatch`.

