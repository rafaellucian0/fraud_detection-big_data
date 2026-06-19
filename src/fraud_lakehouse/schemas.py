"""Schemas and column conventions for the ULB fraud dataset."""

from __future__ import annotations

from typing import Iterable


FEATURE_COLUMNS = ["Time", *[f"V{i}" for i in range(1, 29)], "Amount"]
REQUIRED_COLUMNS = [*FEATURE_COLUMNS, "Class"]


def missing_columns(columns: Iterable[str]) -> list[str]:
    present = set(columns)
    return [column for column in REQUIRED_COLUMNS if column not in present]


def validate_creditcard_columns(columns: Iterable[str]) -> None:
    missing = missing_columns(columns)
    if missing:
        raise ValueError(f"Dataset is missing required columns: {', '.join(missing)}")


def transaction_event_schema():
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    fields = [
        StructField("transaction_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("Time", DoubleType(), True),
        *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
        StructField("Amount", DoubleType(), True),
        StructField("Class", DoubleType(), True),
    ]
    return StructType(fields)
