from fraud_lakehouse.metrics import f1_score, precision, recall


def test_precision() -> None:
    assert precision(8, 2) == 0.8
    assert precision(0, 0) == 0.0


def test_recall() -> None:
    assert recall(9, 1) == 0.9
    assert recall(0, 0) == 0.0


def test_f1_score() -> None:
    assert round(f1_score(0.8, 0.5), 6) == 0.615385
    assert f1_score(0.0, 0.0) == 0.0

