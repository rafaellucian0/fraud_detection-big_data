from _common import make_dag, spark_task


with make_dag("dag_07_data_quality_metrics", "Compute data quality metrics by lakehouse layer.") as dag:
    data_quality = spark_task("data_quality_metrics", "jobs/quality/data_quality_report.py")
