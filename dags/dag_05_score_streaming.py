from _common import make_dag, spark_task


with make_dag("dag_05_score_streaming", "Start Structured Streaming fraud scoring after Silver preparation.") as dag:
    score_stream = spark_task("start_score_stream", "jobs/streaming/score_transactions.py")
