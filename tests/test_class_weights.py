import pytest

from fraud_lakehouse.metrics import class_weights


def test_class_weights_balanced_formula() -> None:
    weights = class_weights(total=1000, legit_count=990, fraud_count=10)
    assert round(weights[0], 6) == round(1000 / (2 * 990), 6)
    assert weights[1] == 50.0


def test_class_weights_require_both_classes() -> None:
    with pytest.raises(ValueError):
        class_weights(total=100, legit_count=100, fraud_count=0)

