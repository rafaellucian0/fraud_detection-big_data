"""Score Silver streaming transactions with the saved RF + GBT MLlib ensemble."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.classification import GBTClassificationModel, RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fraud_lakehouse.model_io import gbt_probability_from_raw, probability_at, read_local_metadata
from fraud_lakehouse.schemas import FEATURE_COLUMNS
from fraud_lakehouse.spark import build_spark


ROOT = Path("/opt/project") if Path("/opt/project").exists() else Path(__file__).resolve().parents[2]


def score_batch(batch_df: DataFrame, batch_id: int, metadata: dict) -> None:
    if batch_df.rdd.isEmpty():
        return
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="keep")
    features = assembler.transform(batch_df)
    rf_model = RandomForestClassificationModel.load(metadata["rf_model_path"])
    gbt_model = GBTClassificationModel.load(metadata["gbt_model_path"])
    threshold = float(metadata["threshold"])
    model_version = metadata["model_version"]

    scored = (
        gbt_model.transform(rf_model.transform(features))
        .withColumnRenamed("rawPrediction", "gbt_raw_prediction")
        .withColumnRenamed("prediction", "gbt_prediction")
    )
    scored = (
        scored.withColumn("rf_fraud_probability", probability_at(F.col("rf_probability"), 1))
        .withColumn("gbt_fraud_probability", gbt_probability_from_raw(F.col("gbt_raw_prediction")))
        .withColumn("fraud_score", (F.col("rf_fraud_probability") + F.col("gbt_fraud_probability")) / F.lit(2.0))
        .withColumn("prediction", (F.col("fraud_score") >= F.lit(threshold)).cast("int"))
        .withColumn("threshold_used", F.lit(threshold))
        .withColumn("model_version", F.lit(model_version))
        .withColumn("scored_at", F.current_timestamp())
        .withColumn("scoring_batch_id", F.lit(int(batch_id)))
    )
    output_columns = [
        "transaction_id",
        "event_timestamp",
        "Amount",
        "label",
        "rf_fraud_probability",
        "gbt_fraud_probability",
        "fraud_score",
        "prediction",
        "threshold_used",
        "model_version",
        "scored_at",
        "scoring_batch_id",
    ]
    scored.select(*output_columns).writeTo("local.gold.fraud_scores").append()
    scored.filter(F.col("prediction") == 1).select(*output_columns).writeTo("local.gold.fraud_alerts").append()

    kafka_payload = scored.select(
        F.col("transaction_id").cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(column) for column in output_columns])).alias("value"),
    )
    kafka_payload.write.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "transactions.scored").save()
    kafka_payload.filter(F.get_json_object("value", "$.prediction") == "1").write.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "fraud.alerts").save()


def main() -> None:
    spark = build_spark("score-transactions-streaming")
    metadata_path = ROOT / "models" / "latest_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError("Train the model first with make train; models/latest_metadata.json was not found.")
    metadata = read_local_metadata(metadata_path)

    stream = spark.readStream.table("local.silver.transactions_clean")
    query = (
        stream.writeStream.foreachBatch(lambda batch_df, batch_id: score_batch(batch_df, batch_id, metadata))
        .option("checkpointLocation", "hdfs://hadoop-namenode:8020/lakehouse/checkpoints/gold/fraud_scores")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
