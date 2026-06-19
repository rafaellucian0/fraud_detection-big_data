from fraud_lakehouse.metrics import transaction_id_from_values


def test_transaction_id_is_deterministic() -> None:
    first = transaction_id_from_values(1, 2, "A")
    second = transaction_id_from_values(1, 2, "A")
    other = transaction_id_from_values(1, 2, "B")
    assert first == second
    assert first != other
    assert len(first) == 64

