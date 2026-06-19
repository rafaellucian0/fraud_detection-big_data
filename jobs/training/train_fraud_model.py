"""Train weighted MLlib RF + GBT ensemble and tune fraud threshold."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from fraud_lakehouse.config import load_model_config, load_threshold_config
from fraud_lakehouse.features import add_class_weight_column
from fraud_lakehouse.metrics import class_weights
from fraud_lakehouse.model_io import gbt_probability_from_raw, probability_at, write_local_metadata
from fraud_lakehouse.schemas import FEATURE_COLUMNS
from fraud_lakehouse.spark import build_spark
from jobs.training.metrics import metrics_for_thresholds
from jobs.training.threshold_tuning import choose_threshold


ROOT = Path("/opt/project") if Path("/opt/project").exists() else Path(__file__).resolve().parents[2]


def main() -> None:
    spark = build_spark("train-fraud-model")
    model_config = load_model_config()
    threshold_config = load_threshold_config()
    model_version = f"{model_config.get('model_version', 'fraud')}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    model_base = f"hdfs://hadoop-namenode:8020/lakehouse/models/fraud_ensemble/{model_version}"

    clean = spark.table("local.silver.transactions_clean").select(*FEATURE_COLUMNS, "label")
    train_df, validation_df = clean.randomSplit([0.8, 0.2], seed=42)
    train_labels = {int(row["label"]) for row in train_df.select("label").distinct().collect()}
    validation_labels = {int(row["label"]) for row in validation_df.select("label").distinct().collect()}
    if train_labels != {0, 1}:
        train_df = clean
    if validation_labels != {0, 1}:
        validation_df = clean

    counts = train_df.groupBy("label").count().collect()
    count_map = {int(row["label"]): int(row["count"]) for row in counts}
    weights = class_weights(sum(count_map.values()), count_map.get(0, 0), count_map.get(1, 0))
    train_df = add_class_weight_column(train_df, weights[0], weights[1])
    validation_df = add_class_weight_column(validation_df, weights[0], weights[1])

    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="keep")
    train_features = assembler.transform(train_df)
    validation_features = assembler.transform(validation_df)

    rf_cfg = model_config.get("random_forest", {})
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="class_weight",
        probabilityCol="rf_probability",
        predictionCol="rf_prediction",
        rawPredictionCol="rf_raw_prediction",
        numTrees=int(rf_cfg.get("num_trees", 80)),
        maxDepth=int(rf_cfg.get("max_depth", 8)),
        seed=int(rf_cfg.get("seed", 42)),
    )
    rf_model = rf.fit(train_features)

    gbt_cfg = model_config.get("gbt", {})
    gbt = GBTClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="class_weight",
        maxIter=int(gbt_cfg.get("max_iter", 50)),
        maxDepth=int(gbt_cfg.get("max_depth", 5)),
        seed=int(gbt_cfg.get("seed", 42)),
    )
    gbt_model = gbt.fit(train_features)

    scored = (
        gbt_model.transform(rf_model.transform(validation_features))
        .withColumnRenamed("rawPrediction", "gbt_raw_prediction")
        .withColumnRenamed("prediction", "gbt_prediction")
    )
    scored = (
        scored.withColumn("rf_fraud_probability", probability_at(F.col("rf_probability"), 1))
        .withColumn("gbt_fraud_probability", gbt_probability_from_raw(F.col("gbt_raw_prediction")))
        .withColumn("fraud_score", (F.col("rf_fraud_probability") + F.col("gbt_fraud_probability")) / F.lit(2.0))
    )

    thresholds = [float(value) for value in threshold_config.get("thresholds", [0.5])]
    threshold_metrics = metrics_for_thresholds(scored, thresholds)
    selected = choose_threshold(
        threshold_metrics,
        policy=str(threshold_config.get("policy", "max_recall_with_precision_floor")),
        min_precision=float(threshold_config.get("min_precision", 0.70)),
    )

    evaluator_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="fraud_score", metricName="areaUnderPR")
    evaluator_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="fraud_score", metricName="areaUnderROC")
    auc_pr = float(evaluator_pr.evaluate(scored))
    roc_auc = float(evaluator_roc.evaluate(scored))

    rf_model.write().overwrite().save(f"{model_base}/rf_model")
    gbt_model.write().overwrite().save(f"{model_base}/gbt_model")

    lr_cfg = model_config.get("logistic_regression_baseline", {})
    if lr_cfg.get("enabled", True):
        lr = LogisticRegression(labelCol="label", featuresCol="features", weightCol="class_weight", maxIter=int(lr_cfg.get("max_iter", 30)))
        lr.fit(train_features).write().overwrite().save(f"{model_base}/lr_baseline")

    metadata = {
        "model_version": model_version,
        "model_base_path": model_base,
        "rf_model_path": f"{model_base}/rf_model",
        "gbt_model_path": f"{model_base}/gbt_model",
        "threshold": selected.threshold,
        "threshold_policy": threshold_config.get("policy", "max_recall_with_precision_floor"),
        "training_timestamp": datetime.now(timezone.utc).isoformat(),
        "feature_columns": FEATURE_COLUMNS,
        "class_weights": {"0": weights[0], "1": weights[1]},
        "metrics": {
            "precision": selected.precision,
            "recall": selected.recall,
            "f1": selected.f1,
            "tp": selected.tp,
            "fp": selected.fp,
            "tn": selected.tn,
            "fn": selected.fn,
            "auc_pr": auc_pr,
            "roc_auc": roc_auc,
        },
    }
    write_local_metadata(ROOT / "models" / "latest_metadata.json", metadata)

    metrics_rows = [
        (
            model_version,
            metric.threshold,
            metric.precision,
            metric.recall,
            metric.f1,
            metric.tp,
            metric.fp,
            metric.tn,
            metric.fn,
            auc_pr if metric.threshold == selected.threshold else None,
            roc_auc if metric.threshold == selected.threshold else None,
            datetime.now(timezone.utc).isoformat(),
        )
        for metric in threshold_metrics
    ]
    spark.createDataFrame(
        metrics_rows,
        [
            "model_version",
            "threshold",
            "precision",
            "recall",
            "f1_score",
            "tp",
            "fp",
            "tn",
            "fn",
            "auc_pr",
            "roc_auc",
            "created_at",
        ],
    ).writeTo("local.gold.model_metrics").append()
    spark.stop()


if __name__ == "__main__":
    main()
