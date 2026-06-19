from _common import make_dag, spark_task


with make_dag("dag_02_bronze_streaming", "Start the Kafka to Bronze Structured Streaming job.") as dag:
    bronze_stream = spark_task("start_bronze_stream", "jobs/streaming/kafka_to_bronze.py")
