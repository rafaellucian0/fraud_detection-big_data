"""Materialize batch scores and fraud alerts after the Silver layer."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.classification import GBTClassificationModel, RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from fraud_lakehouse.model_io import gbt_probability_from_raw, probability_at, read_local_metadata
from fraud_lakehouse.schemas import FEATURE_COLUMNS
from fraud_lakehouse.spark import build_spark


ROOT = Path("/opt/project") if Path("/opt/project").exists() else Path(__file__).resolve().parents[2]


def main() -> None:
    spark = build_spark("score-silver-batch")
    metadata = read_local_metadata(ROOT / "models" / "latest_metadata.json")
    threshold = float(metadata["threshold"])
    silver = spark.table("local.silver.transactions_clean")
    if silver.rdd.isEmpty():
        raise ValueError("Silver table is empty; run the Bronze-to-Silver job before scoring.")

    features = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="keep").transform(silver)
    rf_model = RandomForestClassificationModel.load(metadata["rf_model_path"])
    gbt_model = GBTClassificationModel.load(metadata["gbt_model_path"])
    scored = (
        gbt_model.transform(rf_model.transform(features))
        .withColumnRenamed("rawPrediction", "gbt_raw_prediction")
        .withColumnRenamed("prediction", "gbt_prediction")
        .withColumn("rf_fraud_probability", probability_at(F.col("rf_probability"), 1))
        .withColumn("gbt_fraud_probability", gbt_probability_from_raw(F.col("gbt_raw_prediction")))
        .withColumn("fraud_score", (F.col("rf_fraud_probability") + F.col("gbt_fraud_probability")) / F.lit(2.0))
        .withColumn("prediction", (F.col("fraud_score") >= F.lit(threshold)).cast("int"))
        .withColumn("threshold_used", F.lit(threshold))
        .withColumn("model_version", F.lit(str(metadata["model_version"])))
        .withColumn("scored_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .withColumn("scoring_batch_id", F.lit(0))
    )
    output_columns = [
        "transaction_id", "event_timestamp", "Amount", "label", "rf_fraud_probability",
        "gbt_fraud_probability", "fraud_score", "prediction", "threshold_used",
        "model_version", "scored_at", "scoring_batch_id",
    ]
    scored.select(*output_columns).writeTo("local.gold.fraud_scores").append()
    scored.filter(F.col("prediction") == 1).select(*output_columns).writeTo("local.gold.fraud_alerts").append()
    spark.stop()


if __name__ == "__main__":
    main()
