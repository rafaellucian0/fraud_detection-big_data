"""Pure metric helpers used by training jobs and tests."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


@dataclass(frozen=True)
class BinaryCounts:
    tp: int
    fp: int
    tn: int
    fn: int


@dataclass(frozen=True)
class BinaryMetrics:
    threshold: float
    tp: int
    fp: int
    tn: int
    fn: int
    precision: float
    recall: float
    f1: float


def precision(tp: int, fp: int) -> float:
    denominator = tp + fp
    return 0.0 if denominator == 0 else tp / denominator


def recall(tp: int, fn: int) -> float:
    denominator = tp + fn
    return 0.0 if denominator == 0 else tp / denominator


def f1_score(precision_value: float, recall_value: float) -> float:
    denominator = precision_value + recall_value
    return 0.0 if denominator == 0 else 2 * precision_value * recall_value / denominator


def binary_metrics_from_counts(threshold: float, counts: BinaryCounts) -> BinaryMetrics:
    p = precision(counts.tp, counts.fp)
    r = recall(counts.tp, counts.fn)
    return BinaryMetrics(
        threshold=threshold,
        tp=counts.tp,
        fp=counts.fp,
        tn=counts.tn,
        fn=counts.fn,
        precision=p,
        recall=r,
        f1=f1_score(p, r),
    )


def class_weights(total: int, legit_count: int, fraud_count: int) -> dict[int, float]:
    if total <= 0:
        raise ValueError("total must be greater than zero")
    if legit_count <= 0 or fraud_count <= 0:
        raise ValueError("Both classes must be present to compute balanced class weights.")
    return {
        0: total / (2 * legit_count),
        1: total / (2 * fraud_count),
    }


def transaction_id_from_values(*values: object) -> str:
    import hashlib

    raw = "|".join("" if value is None else str(value) for value in values)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def mean_and_sample_stdev(values: list[float]) -> tuple[float, float]:
    """Return arithmetic mean and sample standard deviation for reported runs."""
    if not values:
        raise ValueError("At least one value is required.")
    mean = sum(values) / len(values)
    if len(values) == 1:
        return mean, 0.0
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return mean, sqrt(variance)
