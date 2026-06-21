"""Run the leakage-aware temporal experiment and publish the deployment model.

The ULB file is chronologically ordered by ``Time``.  Candidate models only see
the first 60% of the stream, tuning uses the next 20%, and the last 20% remains
an untouched test block.  Five seeded fits quantify model stochasticity.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

from pyspark import StorageLevel
from pyspark.ml.classification import GBTClassificationModel, GBTClassifier, RandomForestClassificationModel, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fraud_lakehouse.config import load_experiment_config, load_threshold_config
from fraud_lakehouse.features import add_class_weight_column
from fraud_lakehouse.metrics import BinaryMetrics, class_weights, mean_and_sample_stdev
from fraud_lakehouse.model_io import gbt_probability_from_raw, probability_at, write_local_metadata
from fraud_lakehouse.schemas import FEATURE_COLUMNS
from fraud_lakehouse.spark import build_spark
from jobs.training.metrics import metrics_for_thresholds
from jobs.training.threshold_tuning import choose_threshold


ROOT = Path("/opt/project") if Path("/opt/project").exists() else Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger("temporal_fraud_experiment")


def split_temporally(df: DataFrame, train_fraction: float, validation_fraction: float) -> tuple[DataFrame, DataFrame, DataFrame, tuple[float, float]]:
    """Split an event stream by chronological time, never by random sampling."""
    cut_train, cut_validation = df.approxQuantile(
        "Time", [float(train_fraction), float(train_fraction + validation_fraction)], 0.0001
    )
    if cut_train >= cut_validation:
        raise ValueError("Temporal split cut points are invalid; check the Time column.")
    train = df.filter(F.col("Time") < F.lit(cut_train))
    validation = df.filter((F.col("Time") >= F.lit(cut_train)) & (F.col("Time") < F.lit(cut_validation)))
    test = df.filter(F.col("Time") >= F.lit(cut_validation))
    return train, validation, test, (float(cut_train), float(cut_validation))


def fit_models(train_features: DataFrame, candidate: dict, seed: int):
    rf_config = candidate["random_forest"]
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="class_weight",
        probabilityCol="rf_probability",
        predictionCol="rf_prediction",
        rawPredictionCol="rf_raw_prediction",
        numTrees=int(rf_config["num_trees"]),
        maxDepth=int(rf_config["max_depth"]),
        seed=int(seed),
    )
    gbt_config = candidate["gbt"]
    gbt = GBTClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="class_weight",
        maxIter=int(gbt_config["max_iter"]),
        maxDepth=int(gbt_config["max_depth"]),
        seed=int(seed),
    )
    return rf.fit(train_features), gbt.fit(train_features)


def ensemble_scores(features: DataFrame, rf_model, gbt_model) -> DataFrame:
    scored = (
        gbt_model.transform(rf_model.transform(features))
        .withColumnRenamed("rawPrediction", "gbt_raw_prediction")
        .withColumnRenamed("prediction", "gbt_prediction")
    )
    return (
        scored.withColumn("rf_fraud_probability", probability_at(F.col("rf_probability"), 1))
        .withColumn("gbt_fraud_probability", gbt_probability_from_raw(F.col("gbt_raw_prediction")))
        .withColumn("fraud_score", (F.col("rf_fraud_probability") + F.col("gbt_fraud_probability")) / F.lit(2.0))
        .select("label", "fraud_score")
    )


def evaluate(scored: DataFrame, thresholds: list[float], threshold_config: dict) -> tuple[BinaryMetrics, float, float]:
    cached = scored.persist(StorageLevel.DISK_ONLY)
    cached.count()
    try:
        threshold_metrics = metrics_for_thresholds(cached, thresholds)
        selected = choose_threshold(
            threshold_metrics,
            policy=str(threshold_config.get("policy", "max_recall_with_precision_floor")),
            min_precision=float(threshold_config.get("min_precision", 0.70)),
        )
        auc_pr = float(BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="fraud_score", metricName="areaUnderPR").evaluate(cached))
        roc_auc = float(BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="fraud_score", metricName="areaUnderROC").evaluate(cached))
        return selected, auc_pr, roc_auc
    finally:
        cached.unpersist()


def metric_row(
    experiment_id: str,
    candidate_id: str,
    run_id: int,
    seed: int,
    split_name: str,
    metric: BinaryMetrics,
    auc_pr: float,
    roc_auc: float,
    row_counts: tuple[int, int, int],
    selected_candidate: bool,
    created_at: str,
) -> tuple:
    return (
        experiment_id, candidate_id, run_id, seed, split_name, metric.threshold,
        metric.precision, metric.recall, metric.f1, auc_pr, roc_auc,
        metric.tp, metric.fp, metric.tn, metric.fn,
        row_counts[0], row_counts[1], row_counts[2], selected_candidate, created_at,
    )


def summary_rows(experiment_id: str, candidate_id: str, split_name: str, results: list[dict], selected: bool, created_at: str) -> list[tuple]:
    rows = []
    for metric_name in ["precision", "recall", "f1", "auc_pr", "roc_auc", "threshold"]:
        mean, stdev = mean_and_sample_stdev([float(result[metric_name]) for result in results])
        rows.append((experiment_id, candidate_id, metric_name, split_name, mean, stdev, len(results), selected, created_at))
    return rows


def main() -> None:
    spark = build_spark("temporal-fraud-experiment")
    experiment_config = load_experiment_config()
    threshold_config = load_threshold_config()
    now = datetime.now(timezone.utc)
    experiment_id = f"ulb-temporal-{now.strftime('%Y%m%d%H%M%S')}"
    created_at = now.isoformat()
    candidates = experiment_config["candidates"]
    seeds = [int(seed) for seed in experiment_config["repetitions"]["seeds"]]
    thresholds = [float(value) for value in experiment_config["thresholds"]]
    split_config = experiment_config["split"]
    experiment_base = f"hdfs://hadoop-namenode:8020/lakehouse/models/experiments/{experiment_id}"

    source = spark.table("local.silver.transactions_clean").dropDuplicates(["transaction_id"])
    source = source.filter(F.col("label").isin(0.0, 1.0))
    train, validation, test, cuts = split_temporally(
        source, float(split_config["train_fraction"]), float(split_config["validation_fraction"])
    )
    train = train.persist(StorageLevel.DISK_ONLY)
    validation = validation.persist(StorageLevel.DISK_ONLY)
    test = test.persist(StorageLevel.DISK_ONLY)
    try:
        row_counts = (train.count(), validation.count(), test.count())
        if min(row_counts) == 0:
            raise ValueError(f"Temporal split produced an empty partition: {row_counts}")
        train_labels = {int(row["label"]) for row in train.select("label").distinct().collect()}
        validation_labels = {int(row["label"]) for row in validation.select("label").distinct().collect()}
        test_labels = {int(row["label"]) for row in test.select("label").distinct().collect()}
        if train_labels != {0, 1} or validation_labels != {0, 1} or test_labels != {0, 1}:
            raise ValueError("Each temporal partition must contain legitimate and fraudulent transactions.")

        train_counts = {int(row["label"]): int(row["count"]) for row in train.groupBy("label").count().collect()}
        weights = class_weights(row_counts[0], train_counts[0], train_counts[1])
        assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="keep")
        train_features = assembler.transform(add_class_weight_column(train, weights[0], weights[1])).persist(StorageLevel.DISK_ONLY)
        validation_features = assembler.transform(validation).persist(StorageLevel.DISK_ONLY)
        test_features = assembler.transform(test).persist(StorageLevel.DISK_ONLY)
        train_features.count()
        validation_features.count()
        test_features.count()

        validation_results: dict[str, list[dict]] = {candidate["id"]: [] for candidate in candidates}
        metric_rows = []
        for candidate in candidates:
            candidate_id = str(candidate["id"])
            for run_id, seed in enumerate(seeds, start=1):
                LOGGER.warning("Validation fit: candidate=%s run=%s seed=%s", candidate_id, run_id, seed)
                rf_model, gbt_model = fit_models(train_features, candidate, seed)
                model_path = f"{experiment_base}/{candidate_id}/seed-{seed}"
                rf_model.write().overwrite().save(f"{model_path}/rf_model")
                gbt_model.write().overwrite().save(f"{model_path}/gbt_model")
                metric, auc_pr, roc_auc = evaluate(ensemble_scores(validation_features, rf_model, gbt_model), thresholds, threshold_config)
                result = {
                    "run_id": run_id, "seed": seed, "threshold": metric.threshold,
                    "precision": metric.precision, "recall": metric.recall, "f1": metric.f1,
                    "auc_pr": auc_pr, "roc_auc": roc_auc, "metric": metric,
                }
                validation_results[candidate_id].append(result)
                metric_rows.append(metric_row(experiment_id, candidate_id, run_id, seed, "validation", metric, auc_pr, roc_auc, row_counts, False, created_at))

        champion_id = max(
            validation_results,
            key=lambda candidate_id: (
                mean_and_sample_stdev([item["auc_pr"] for item in validation_results[candidate_id]])[0],
                mean_and_sample_stdev([item["f1"] for item in validation_results[candidate_id]])[0],
            ),
        )
        champion = next(candidate for candidate in candidates if candidate["id"] == champion_id)
        LOGGER.warning("Selected candidate by mean validation PR-AUC: %s", champion_id)
        metric_rows = [
            row[:18] + (row[1] == champion_id,) + row[19:]
            for row in metric_rows
        ]

        test_results = []
        for result in validation_results[champion_id]:
            model_path = f"{experiment_base}/{champion_id}/seed-{result['seed']}"
            rf_model = RandomForestClassificationModel.load(f"{model_path}/rf_model")
            gbt_model = GBTClassificationModel.load(f"{model_path}/gbt_model")
            test_metric, test_auc_pr, test_roc_auc = evaluate(
                ensemble_scores(test_features, rf_model, gbt_model), [result["threshold"]], threshold_config
            )
            test_result = {
                "run_id": result["run_id"], "seed": result["seed"], "threshold": test_metric.threshold,
                "precision": test_metric.precision, "recall": test_metric.recall, "f1": test_metric.f1,
                "auc_pr": test_auc_pr, "roc_auc": test_roc_auc, "metric": test_metric,
            }
            test_results.append(test_result)
            metric_rows.append(metric_row(
                experiment_id, champion_id, result["run_id"], result["seed"], "test", test_metric,
                test_auc_pr, test_roc_auc, row_counts, True, created_at,
            ))

        metric_schema = [
            "experiment_id", "candidate_id", "run_id", "seed", "split_name", "threshold",
            "precision", "recall", "f1_score", "auc_pr", "roc_auc", "tp", "fp", "tn", "fn",
            "train_rows", "validation_rows", "test_rows", "selected_candidate", "created_at",
        ]
        spark.createDataFrame(metric_rows, metric_schema).writeTo("local.gold.experiment_metrics").append()
        summaries = []
        for candidate_id, results in validation_results.items():
            summaries.extend(summary_rows(experiment_id, candidate_id, "validation", results, candidate_id == champion_id, created_at))
        summaries.extend(summary_rows(experiment_id, champion_id, "test", test_results, True, created_at))
        spark.createDataFrame(
            summaries,
            ["experiment_id", "candidate_id", "metric_name", "split_name", "mean_value", "sample_stdev", "runs", "selected_candidate", "created_at"],
        ).writeTo("local.gold.experiment_summary").append()

        # The deployment model may use train+validation only after test reporting is complete.
        deployment_train = train.unionByName(validation)
        deployment_counts = {int(row["label"]): int(row["count"]) for row in deployment_train.groupBy("label").count().collect()}
        deployment_weights = class_weights(row_counts[0] + row_counts[1], deployment_counts[0], deployment_counts[1])
        deployment_features = assembler.transform(add_class_weight_column(deployment_train, deployment_weights[0], deployment_weights[1]))
        deployment_seed = seeds[0]
        deployment_rf, deployment_gbt = fit_models(deployment_features, champion, deployment_seed)
        model_version = f"{experiment_id}-deployment"
        deployment_base = f"hdfs://hadoop-namenode:8020/lakehouse/models/fraud_ensemble/{model_version}"
        deployment_rf.write().overwrite().save(f"{deployment_base}/rf_model")
        deployment_gbt.write().overwrite().save(f"{deployment_base}/gbt_model")
        deployment_threshold = float(median([item["threshold"] for item in validation_results[champion_id]]))
        metadata = {
            "model_version": model_version,
            "model_base_path": deployment_base,
            "rf_model_path": f"{deployment_base}/rf_model",
            "gbt_model_path": f"{deployment_base}/gbt_model",
            "threshold": deployment_threshold,
            "threshold_policy": threshold_config.get("policy", "max_recall_with_precision_floor"),
            "training_timestamp": datetime.now(timezone.utc).isoformat(),
            "feature_columns": FEATURE_COLUMNS,
            "class_weights": {"0": deployment_weights[0], "1": deployment_weights[1]},
            "experiment": {
                "experiment_id": experiment_id,
                "protocol": "chronological 60/20/20 split; validation-only tuning; five seeded repetitions",
                "time_cut_points": {"train_validation": cuts[0], "validation_test": cuts[1]},
                "split_rows": {"train": row_counts[0], "validation": row_counts[1], "test": row_counts[2]},
                "candidate_id": champion_id,
                "seeds": seeds,
                "test_summary": {
                    name: mean_and_sample_stdev([result[name] for result in test_results])[0]
                    for name in ["precision", "recall", "f1", "auc_pr", "roc_auc"]
                },
            },
        }
        write_local_metadata(ROOT / "models" / "latest_metadata.json", metadata)
        LOGGER.warning("Experiment complete: id=%s candidate=%s rows=%s", experiment_id, champion_id, row_counts)
    finally:
        train.unpersist()
        validation.unpersist()
        test.unpersist()
        try:
            train_features.unpersist()
            validation_features.unpersist()
            test_features.unpersist()
        except UnboundLocalError:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
