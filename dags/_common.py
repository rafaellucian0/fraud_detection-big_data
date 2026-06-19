"""Shared Airflow DAG helpers."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {"owner": "fraud-lakehouse", "depends_on_past": False, "retries": 0}
PROJECT_DIR = "/opt/project"
SPARK_SUBMIT = (
    "docker exec fraud-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
    "--conf spark.executorEnv.PYTHONPATH=/opt/project/src:/opt/project "
    "--conf spark.driverEnv.PYTHONPATH=/opt/project/src:/opt/project"
)


def make_dag(dag_id: str, description: str) -> DAG:
    return DAG(
        dag_id=dag_id,
        description=description,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["fraud", "lakehouse", "spark", "mllib"],
    )


def make_task(task_id: str, command: str) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=command)


def spark_task(task_id: str, job_path: str) -> BashOperator:
    return make_task(task_id, f"{SPARK_SUBMIT} /opt/project/{job_path}")


def spark_python_task(task_id: str, command: str) -> BashOperator:
    return make_task(task_id, f"docker exec fraud-spark-master bash -lc 'cd /opt/project && {command}'")
