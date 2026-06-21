-- Count records by layer.
SELECT 'bronze' AS layer, count(*) AS records FROM local.bronze.transactions_raw
UNION ALL
SELECT 'silver_clean' AS layer, count(*) AS records FROM local.silver.transactions_clean
UNION ALL
SELECT 'gold_scores' AS layer, count(*) AS records FROM local.gold.fraud_scores;

-- Dashboard: fraud volume by hour.
SELECT
  metric_window,
  total_transactions,
  predicted_frauds,
  predicted_fraud_rate,
  suspicious_amount,
  p95_fraud_score
FROM local.gold.fraud_kpis_hourly
ORDER BY metric_window DESC;

-- Dashboard: score distribution.
SELECT
  width_bucket(fraud_score, 0.0, 1.0, 10) AS score_bucket,
  count(*) AS transactions
FROM local.gold.fraud_scores
GROUP BY width_bucket(fraud_score, 0.0, 1.0, 10)
ORDER BY score_bucket;

-- Latest model metrics.
SELECT *
FROM local.gold.model_metrics
ORDER BY created_at DESC, threshold;

-- Temporal experiment: compare candidates using validation only.
SELECT
  candidate_id,
  split_name,
  metric_name,
  mean_value,
  sample_stdev,
  runs,
  selected_candidate
FROM local.gold.experiment_summary
ORDER BY split_name, candidate_id, metric_name;

-- Independent test runs of the selected candidate.
SELECT
  run_id,
  seed,
  threshold,
  precision,
  recall,
  f1_score,
  auc_pr,
  roc_auc,
  tp,
  fp,
  tn,
  fn
FROM local.gold.experiment_metrics
WHERE split_name = 'test'
ORDER BY run_id;

-- Data quality.
SELECT *
FROM local.gold.data_quality_metrics
ORDER BY created_at DESC, table_name, metric_name;
