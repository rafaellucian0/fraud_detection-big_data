"""Create HDFS directories, Iceberg namespaces and tables."""

from __future__ import annotations

from pathlib import Path

from fraud_lakehouse.iceberg_utils import ensure_hdfs_layout, execute_sql_file
from fraud_lakehouse.spark import build_spark


ROOT = Path("/opt/project") if Path("/opt/project").exists() else Path(__file__).resolve().parents[2]


def main() -> None:
    spark = build_spark("create-iceberg-tables")
    ensure_hdfs_layout(spark)
    for sql_file in [
        "create_namespaces.sql",
        "create_bronze_tables.sql",
        "create_silver_tables.sql",
        "create_gold_tables.sql",
    ]:
        execute_sql_file(spark, ROOT / "sql" / sql_file)
    spark.stop()


if __name__ == "__main__":
    main()

