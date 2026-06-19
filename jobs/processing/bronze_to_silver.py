"""Batch Bronze to Silver transformation with schema enforcement and features."""

from __future__ import annotations

from fraud_lakehouse.features import add_class_weight_column, add_feature_array_column, clean_creditcard_dataframe
from fraud_lakehouse.metrics import class_weights
from fraud_lakehouse.schemas import FEATURE_COLUMNS
from fraud_lakehouse.spark import build_spark


SILVER_CLEAN_COLUMNS = [
    "transaction_id",
    "event_time",
    "event_timestamp",
    *FEATURE_COLUMNS,
    "Class",
    "label",
    "class_weight",
    "ingestion_timestamp",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "source_file",
    "raw_event_id",
]

SILVER_FEATURE_COLUMNS = [
    *SILVER_CLEAN_COLUMNS[: SILVER_CLEAN_COLUMNS.index("ingestion_timestamp")],
    "feature_array",
    *SILVER_CLEAN_COLUMNS[SILVER_CLEAN_COLUMNS.index("ingestion_timestamp") :],
]


def main() -> None:
    spark = build_spark("bronze-to-silver")
    bronze = spark.table("local.bronze.transactions_raw")
    clean = clean_creditcard_dataframe(bronze)
    counts = clean.groupBy("label").count().collect()
    count_map = {int(row["label"]): int(row["count"]) for row in counts}
    weights = class_weights(sum(count_map.values()), count_map.get(0, 0), count_map.get(1, 0))
    weighted = add_class_weight_column(clean, weights[0], weights[1])
    features = add_feature_array_column(weighted)
    weighted.select(*SILVER_CLEAN_COLUMNS).writeTo("local.silver.transactions_clean").append()
    features.select(*SILVER_FEATURE_COLUMNS).writeTo("local.silver.transactions_features").append()
    spark.stop()


if __name__ == "__main__":
    main()
