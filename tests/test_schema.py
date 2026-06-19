import pytest

from fraud_lakehouse.schemas import FEATURE_COLUMNS, missing_columns, validate_creditcard_columns


def test_feature_columns_match_ulb_dataset() -> None:
    assert FEATURE_COLUMNS[0] == "Time"
    assert FEATURE_COLUMNS[-1] == "Amount"
    assert len(FEATURE_COLUMNS) == 30


def test_missing_columns() -> None:
    columns = ["Time", "Amount", "Class"]
    missing = missing_columns(columns)
    assert "V1" in missing
    assert "Class" not in missing


def test_validate_creditcard_columns_raises() -> None:
    with pytest.raises(ValueError):
        validate_creditcard_columns(["Time", "Amount"])

