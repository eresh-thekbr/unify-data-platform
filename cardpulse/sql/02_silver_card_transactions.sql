-- Validated, deduplicated transactions with the card number masked.
CREATE TABLE IF NOT EXISTS workspace.cardpulse.silver_card_transactions (
  transaction_id STRING,
  card_number_masked STRING COMMENT 'Only the last 4 digits are visible',
  merchant_name STRING,
  merchant_category STRING COMMENT 'Trimmed and lower case',
  amount DECIMAL(12, 2),
  transaction_ts TIMESTAMP COMMENT 'Standardized to UTC',
  transaction_date DATE,
  source_file STRING,
  run_id STRING,
  processed_ts TIMESTAMP
)
USING ICEBERG
COMMENT 'Silver: clean card transactions';
