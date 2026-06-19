from _common import make_dag, spark_task


with make_dag("dag_04_train_fraud_model", "Train weighted MLlib models and tune ensemble threshold.") as dag:
    train_model = spark_task("train_fraud_model", "jobs/training/train_fraud_model.py")
