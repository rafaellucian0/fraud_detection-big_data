"""Build OLAP Gold aggregations and local PowerBI exports."""

from __future__ import annotations

from fraud_lakehouse.spark import build_spark


def main() -> None:
    spark = build_spark("build-gold-aggregations")
    scores = spark.table("local.gold.fraud_scores")
    scores.createOrReplaceTempView("fraud_scores")

    hourly = spark.sql(
        """
        SELECT
          date_trunc('hour', event_timestamp) AS metric_window,
          count(*) AS total_transactions,
          sum(prediction) AS predicted_frauds,
          sum(CASE WHEN prediction = 1 THEN Amount ELSE 0 END) AS suspicious_amount,
          avg(fraud_score) AS avg_fraud_score,
          percentile_approx(fraud_score, 0.95) AS p95_fraud_score,
          sum(prediction) / count(*) AS predicted_fraud_rate
        FROM fraud_scores
        GROUP BY date_trunc('hour', event_timestamp)
        """
    )
    daily = spark.sql(
        """
        SELECT
          date_trunc('day', event_timestamp) AS metric_date,
          count(*) AS total_transactions,
          sum(prediction) AS predicted_frauds,
          sum(CASE WHEN prediction = 1 THEN Amount ELSE 0 END) AS suspicious_amount,
          avg(fraud_score) AS avg_fraud_score,
          percentile_approx(fraud_score, 0.95) AS p95_fraud_score,
          sum(prediction) / count(*) AS predicted_fraud_rate
        FROM fraud_scores
        GROUP BY date_trunc('day', event_timestamp)
        """
    )
    hourly.writeTo("local.gold.fraud_kpis_hourly").append()
    daily.writeTo("local.gold.fraud_kpis_daily").append()

    hourly.coalesce(1).write.mode("overwrite").option("header", True).csv(
        "file:///opt/project/data/output/powerbi/fraud_kpis_hourly"
    )
    daily.coalesce(1).write.mode("overwrite").option("header", True).csv(
        "file:///opt/project/data/output/powerbi/fraud_kpis_daily"
    )
    scores.coalesce(1).write.mode("overwrite").option("header", True).csv(
        "file:///opt/project/data/output/powerbi/fraud_scores"
    )
    spark.stop()


if __name__ == "__main__":
    main()
