"""Write simple data quality metrics for lakehouse layers."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from fraud_lakehouse.schemas import REQUIRED_COLUMNS
from fraud_lakehouse.spark import build_spark


TABLES = [
    "local.bronze.transactions_raw",
    "local.silver.transactions_clean",
    "local.silver.transactions_features",
    "local.gold.fraud_scores",
]


def main() -> None:
    spark = build_spark("data-quality-report")
    rows = []
    created_at = datetime.now(timezone.utc).isoformat()
    for table in TABLES:
        df = spark.table(table)
        total = df.count()
        rows.append((table, "row_count", "*", float(total), created_at))
        for column in df.columns:
            nulls = df.filter(F.col(column).isNull()).count()
            rows.append((table, "null_count", column, float(nulls), created_at))
        if "transaction_id" in df.columns:
            duplicates = df.groupBy("transaction_id").count().filter(F.col("count") > 1).count()
            rows.append((table, "duplicate_transaction_id", "transaction_id", float(duplicates), created_at))
        if "Class" in df.columns:
            for row in df.groupBy("Class").count().collect():
                rows.append((table, "class_distribution", str(row["Class"]), float(row["count"]), created_at))
        if table.endswith("transactions_raw"):
            missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
            rows.append((table, "schema_missing_required_columns", ",".join(missing) or "none", float(len(missing)), created_at))

    spark.createDataFrame(rows, ["table_name", "metric_name", "metric_dimension", "metric_value", "created_at"]).writeTo(
        "local.gold.data_quality_metrics"
    ).append()
    spark.stop()


if __name__ == "__main__":
    main()

