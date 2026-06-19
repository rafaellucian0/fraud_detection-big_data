"""Threshold selection for the RF + GBT fraud ensemble."""

from __future__ import annotations

from fraud_lakehouse.metrics import BinaryMetrics


def choose_threshold(
    metrics: list[BinaryMetrics],
    policy: str = "max_recall_with_precision_floor",
    min_precision: float = 0.70,
) -> BinaryMetrics:
    if not metrics:
        raise ValueError("At least one threshold metric is required.")

    if policy == "best_f1":
        return max(metrics, key=lambda item: (item.f1, item.recall, item.precision))

    if policy in {"recall_at_min_precision", "max_recall_with_precision_floor"}:
        eligible = [item for item in metrics if item.precision >= min_precision]
        if eligible:
            return max(eligible, key=lambda item: (item.recall, item.f1, item.precision))
        return max(metrics, key=lambda item: (item.f1, item.recall, item.precision))

    raise ValueError(f"Unsupported threshold policy: {policy}")

