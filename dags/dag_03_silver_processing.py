from _common import make_dag, spark_task


with make_dag("dag_03_silver_processing", "Transform Bronze records into Silver clean/features tables.") as dag:
    silver_processing = spark_task("silver_processing", "jobs/processing/bronze_to_silver.py")
