from _common import make_dag, spark_task


with make_dag("dag_00_init_lakehouse", "Create HDFS layout, Iceberg namespaces and tables.") as dag:
    init_lakehouse = spark_task("init_lakehouse", "jobs/init/create_iceberg_tables.py")
