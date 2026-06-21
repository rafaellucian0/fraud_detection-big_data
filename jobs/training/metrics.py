"""Metric helpers for Spark model evaluation."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fraud_lakehouse.metrics import BinaryCounts, binary_metrics_from_counts


def counts_for_threshold(df: DataFrame, threshold: float, score_col: str = "fraud_score", label_col: str = "label") -> BinaryCounts:
    evaluated = df.withColumn("predicted_label", (F.col(score_col) >= F.lit(float(threshold))).cast("int"))
    row = evaluated.agg(
        F.sum(((F.col("predicted_label") == 1) & (F.col(label_col) == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("predicted_label") == 1) & (F.col(label_col) == 0)).cast("int")).alias("fp"),
        F.sum(((F.col("predicted_label") == 0) & (F.col(label_col) == 0)).cast("int")).alias("tn"),
        F.sum(((F.col("predicted_label") == 0) & (F.col(label_col) == 1)).cast("int")).alias("fn"),
    ).first()
    return BinaryCounts(tp=int(row.tp or 0), fp=int(row.fp or 0), tn=int(row.tn or 0), fn=int(row.fn or 0))


def counts_for_thresholds(
    df: DataFrame,
    thresholds: list[float],
    score_col: str = "fraud_score",
    label_col: str = "label",
) -> dict[float, BinaryCounts]:
    """Compute all confusion matrices in one Spark aggregation."""
    if not thresholds:
        return {}
    expressions = []
    for index, threshold in enumerate(thresholds):
        predicted = F.col(score_col) >= F.lit(float(threshold))
        expressions.extend(
            [
                F.sum((predicted & (F.col(label_col) == 1)).cast("long")).alias(f"tp_{index}"),
                F.sum((predicted & (F.col(label_col) == 0)).cast("long")).alias(f"fp_{index}"),
                F.sum(((~predicted) & (F.col(label_col) == 0)).cast("long")).alias(f"tn_{index}"),
                F.sum(((~predicted) & (F.col(label_col) == 1)).cast("long")).alias(f"fn_{index}"),
            ]
        )
    row = df.agg(*expressions).first()
    return {
        float(threshold): BinaryCounts(
            tp=int(row[f"tp_{index}"] or 0),
            fp=int(row[f"fp_{index}"] or 0),
            tn=int(row[f"tn_{index}"] or 0),
            fn=int(row[f"fn_{index}"] or 0),
        )
        for index, threshold in enumerate(thresholds)
    }


def metrics_for_thresholds(df: DataFrame, thresholds: list[float], score_col: str = "fraud_score", label_col: str = "label"):
    counts = counts_for_thresholds(df, thresholds, score_col, label_col)
    return [binary_metrics_from_counts(threshold, counts[float(threshold)]) for threshold in thresholds]
