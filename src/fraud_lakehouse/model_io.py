"""Model metadata and probability helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.ml.linalg import VectorUDT
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def probability_at(probability_col: Column, index: int = 1) -> Column:
    return F.udf(lambda vector: float(vector[index]) if vector is not None else 0.0, DoubleType())(probability_col)


def gbt_probability_from_raw(raw_prediction_col: Column) -> Column:
    import math

    def to_probability(vector):
        if vector is None:
            return 0.0
        margin = float(vector[1])
        return float(1.0 / (1.0 + math.exp(-2.0 * margin)))

    return F.udf(to_probability, DoubleType())(raw_prediction_col)


def write_local_metadata(path: str | Path, metadata: dict[str, Any]) -> None:
    metadata_path = Path(path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def read_local_metadata(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _vector_udt_marker() -> VectorUDT:
    return VectorUDT()
