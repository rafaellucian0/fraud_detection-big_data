"""Kafka serialization helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from fraud_lakehouse.metrics import transaction_id_from_values
from fraud_lakehouse.schemas import FEATURE_COLUMNS


def row_to_event(row: dict[str, Any], source_file: str = "creditcard.csv") -> dict[str, Any]:
    values = [row.get(column) for column in FEATURE_COLUMNS]
    event = {
        "transaction_id": row.get("transaction_id") or transaction_id_from_values(*values),
        "event_time": row.get("event_time") or datetime.now(timezone.utc).isoformat(),
        "source_file": source_file,
    }
    for column in FEATURE_COLUMNS:
        event[column] = float(row[column]) if row.get(column) not in (None, "") else None
    event["Class"] = float(row["Class"]) if row.get("Class") not in (None, "") else None
    return event


def json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

