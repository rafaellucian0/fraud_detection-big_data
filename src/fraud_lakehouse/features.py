"""Feature engineering helpers for Spark DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler

from fraud_lakehouse.schemas import FEATURE_COLUMNS


def ensure_transaction_id(df: DataFrame) -> DataFrame:
    if "transaction_id" in df.columns:
        return df.withColumn(
            "transaction_id",
            F.coalesce(F.col("transaction_id"), F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in FEATURE_COLUMNS]), 256)),
        )
    return df.withColumn(
        "transaction_id",
        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in FEATURE_COLUMNS]), 256),
    )


def clean_creditcard_dataframe(df: DataFrame) -> DataFrame:
    cleaned = df
    for column in FEATURE_COLUMNS + ["Class"]:
        cleaned = cleaned.withColumn(column, F.col(column).cast("double"))
    cleaned = ensure_transaction_id(cleaned)
    cleaned = cleaned.withColumn("event_timestamp", F.coalesce(F.to_timestamp("event_time"), F.current_timestamp()))
    cleaned = cleaned.withColumn("label", F.col("Class").cast("double"))
    cleaned = cleaned.fillna({column: 0.0 for column in FEATURE_COLUMNS})
    cleaned = cleaned.filter(F.col("label").isin(0.0, 1.0))
    return cleaned


def add_class_weight_column(df: DataFrame, legit_weight: float, fraud_weight: float) -> DataFrame:
    return df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, F.lit(float(fraud_weight))).otherwise(F.lit(float(legit_weight))),
    )


def add_features_column(df: DataFrame, output_col: str = "features") -> DataFrame:
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol=output_col, handleInvalid="keep")
    return assembler.transform(df)


def add_feature_array_column(df: DataFrame, output_col: str = "feature_array") -> DataFrame:
    return df.withColumn(output_col, F.array(*[F.col(column).cast("double") for column in FEATURE_COLUMNS]))
