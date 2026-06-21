CREATE TABLE IF NOT EXISTS local.gold.fraud_scores (
  transaction_id STRING,
  event_timestamp TIMESTAMP,
  Amount DOUBLE,
  label DOUBLE,
  rf_fraud_probability DOUBLE,
  gbt_fraud_probability DOUBLE,
  fraud_score DOUBLE,
  prediction INT,
  threshold_used DOUBLE,
  model_version STRING,
  scored_at TIMESTAMP,
  scoring_batch_id INT
) USING iceberg
PARTITIONED BY (days(scored_at));

CREATE TABLE IF NOT EXISTS local.gold.fraud_alerts (
  transaction_id STRING,
  event_timestamp TIMESTAMP,
  Amount DOUBLE,
  label DOUBLE,
  rf_fraud_probability DOUBLE,
  gbt_fraud_probability DOUBLE,
  fraud_score DOUBLE,
  prediction INT,
  threshold_used DOUBLE,
  model_version STRING,
  scored_at TIMESTAMP,
  scoring_batch_id INT
) USING iceberg
PARTITIONED BY (days(scored_at));

CREATE TABLE IF NOT EXISTS local.gold.fraud_kpis_hourly (
  metric_window TIMESTAMP,
  total_transactions BIGINT,
  predicted_frauds BIGINT,
  suspicious_amount DOUBLE,
  avg_fraud_score DOUBLE,
  p95_fraud_score DOUBLE,
  predicted_fraud_rate DOUBLE
) USING iceberg
PARTITIONED BY (days(metric_window));

CREATE TABLE IF NOT EXISTS local.gold.fraud_kpis_daily (
  metric_date TIMESTAMP,
  total_transactions BIGINT,
  predicted_frauds BIGINT,
  suspicious_amount DOUBLE,
  avg_fraud_score DOUBLE,
  p95_fraud_score DOUBLE,
  predicted_fraud_rate DOUBLE
) USING iceberg
PARTITIONED BY (days(metric_date));

CREATE TABLE IF NOT EXISTS local.gold.model_metrics (
  model_version STRING,
  threshold DOUBLE,
  precision DOUBLE,
  recall DOUBLE,
  f1_score DOUBLE,
  tp BIGINT,
  fp BIGINT,
  tn BIGINT,
  fn BIGINT,
  auc_pr DOUBLE,
  roc_auc DOUBLE,
  created_at STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS local.gold.data_quality_metrics (
  table_name STRING,
  metric_name STRING,
  metric_dimension STRING,
  metric_value DOUBLE,
  created_at STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS local.gold.experiment_metrics (
  experiment_id STRING,
  candidate_id STRING,
  run_id INT,
  seed INT,
  split_name STRING,
  threshold DOUBLE,
  precision DOUBLE,
  recall DOUBLE,
  f1_score DOUBLE,
  auc_pr DOUBLE,
  roc_auc DOUBLE,
  tp BIGINT,
  fp BIGINT,
  tn BIGINT,
  fn BIGINT,
  train_rows BIGINT,
  validation_rows BIGINT,
  test_rows BIGINT,
  selected_candidate BOOLEAN,
  created_at STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS local.gold.experiment_summary (
  experiment_id STRING,
  candidate_id STRING,
  metric_name STRING,
  split_name STRING,
  mean_value DOUBLE,
  sample_stdev DOUBLE,
  runs INT,
  selected_candidate BOOLEAN,
  created_at STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS local.ml.model_metrics (
  model_version STRING,
  metric_name STRING,
  metric_value DOUBLE,
  created_at STRING
) USING iceberg;
