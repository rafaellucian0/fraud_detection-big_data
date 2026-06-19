from _common import make_dag, spark_task


with make_dag("dag_06_gold_aggregations", "Build Gold KPIs and PowerBI exports.") as dag:
    gold_aggregations = spark_task("gold_aggregations", "jobs/gold/build_gold_aggregations.py")
