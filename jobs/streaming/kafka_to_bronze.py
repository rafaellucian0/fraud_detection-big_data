"""Kafka transactions.raw to Iceberg Bronze streaming job."""

from __future__ import annotations

from pyspark.sql import functions as F

from fraud_lakehouse.schemas import transaction_event_schema
from fraud_lakehouse.spark import build_spark


BRONZE_COLUMNS = [
    "transaction_id",
    "event_time",
    "Time",
    *[f"V{i}" for i in range(1, 29)],
    "Amount",
    "Class",
    "ingestion_timestamp",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "source_file",
    "raw_event_id",
]


def main() -> None:
    spark = build_spark("kafka-to-bronze")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "transactions.raw")
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), transaction_event_schema()).alias("event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("ingestion_timestamp"),
    ).select("event.*", "ingestion_timestamp", "kafka_topic", "kafka_partition", "kafka_offset")
    bronze = parsed.withColumn("raw_event_id", F.sha2(F.concat_ws("|", F.col("transaction_id"), F.col("kafka_offset")), 256)).select(
        *BRONZE_COLUMNS
    )

    query = (
        bronze.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "hdfs://hadoop-namenode:8020/lakehouse/checkpoints/bronze/transactions_raw")
        .toTable("local.bronze.transactions_raw")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
