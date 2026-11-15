-- Daily spend per merchant category, rebuilt from silver on every run.
CREATE TABLE IF NOT EXISTS workspace.cardpulse.gold_daily_spend_by_category (
  transaction_date DATE,
  merchant_category STRING,
  transaction_count BIGINT,
  total_spend_amount DECIMAL(18, 2),
  average_spend_amount DECIMAL(12, 2),
  refreshed_ts TIMESTAMP
)
USING ICEBERG
COMMENT 'Gold: daily spend by merchant category';
