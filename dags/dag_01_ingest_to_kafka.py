from _common import make_dag, spark_python_task


with make_dag("dag_01_ingest_to_kafka", "Validate dataset and simulate transaction arrival in Kafka.") as dag:
    validate_dataset = spark_python_task(
        "validate_dataset",
        "python3 /opt/project/jobs/ingestion/produce_transactions.py --validate-only --input /opt/project/data/input/creditcard.csv",
    )
    ingest_sample = spark_python_task(
        "ingest_sample",
        "python3 /opt/project/jobs/ingestion/produce_transactions.py --input /opt/project/data/input/creditcard.csv --bootstrap-servers kafka:9092 --topic transactions.raw --batch-size 100 --sleep-seconds 1 --max-records 1000",
    )

    validate_dataset >> ingest_sample
