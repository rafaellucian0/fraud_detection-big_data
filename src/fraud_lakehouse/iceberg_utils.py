"""Iceberg SQL helpers."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def execute_sql_file(spark: SparkSession, path: str | Path) -> None:
    statements = [
        statement.strip()
        for statement in Path(path).read_text(encoding="utf-8").split(";")
        if statement.strip()
    ]
    for statement in statements:
        spark.sql(statement)


def ensure_hdfs_layout(spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    for path in [
        "/lakehouse/warehouse",
        "/lakehouse/checkpoints",
        "/lakehouse/models",
        "/lakehouse/raw",
        "/lakehouse/tmp",
    ]:
        fs.mkdirs(jvm.org.apache.hadoop.fs.Path(path))

