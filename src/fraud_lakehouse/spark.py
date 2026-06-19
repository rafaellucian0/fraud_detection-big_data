"""Spark session construction."""

from __future__ import annotations

from pyspark.sql import SparkSession


ICEBERG_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1",
        "org.postgresql:postgresql:42.7.3",
    ]
)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ICEBERG_PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hive")
        .config("spark.sql.catalog.local.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.local.warehouse", "hdfs://hadoop-namenode:8020/lakehouse/warehouse")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020")
        .getOrCreate()
    )

