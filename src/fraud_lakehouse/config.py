"""Configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "configs" / "app"


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return an empty dict for empty documents."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_pipeline_config() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "pipeline.yaml")


def load_model_config() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "model.yaml")


def load_threshold_config() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "thresholds.yaml")


def load_experiment_config() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "experiment.yaml")


def require_dataset(path: str | Path) -> Path:
    dataset_path = Path(path)
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset not found at {dataset_path}. Place the ULB creditcard.csv file there."
        )
    if dataset_path.name.lower() != "creditcard.csv":
        raise ValueError("Expected the ULB dataset file to be named creditcard.csv.")
    return dataset_path
