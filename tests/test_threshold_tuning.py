from fraud_lakehouse.metrics import BinaryMetrics
from jobs.training.threshold_tuning import choose_threshold


def metric(threshold: float, precision: float, recall: float, f1: float) -> BinaryMetrics:
    return BinaryMetrics(threshold, 1, 1, 1, 1, precision, recall, f1)


def test_choose_threshold_prefers_recall_with_precision_floor() -> None:
    selected = choose_threshold(
        [
            metric(0.2, precision=0.60, recall=0.95, f1=0.73),
            metric(0.4, precision=0.72, recall=0.88, f1=0.79),
            metric(0.6, precision=0.90, recall=0.75, f1=0.82),
        ],
        min_precision=0.70,
    )
    assert selected.threshold == 0.4


def test_choose_threshold_falls_back_to_best_f1() -> None:
    selected = choose_threshold(
        [
            metric(0.2, precision=0.40, recall=0.95, f1=0.56),
            metric(0.4, precision=0.50, recall=0.75, f1=0.60),
        ],
        min_precision=0.70,
    )
    assert selected.threshold == 0.4

