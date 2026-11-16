-- Rows that failed validation or were duplicates, with the reason they were rejected.
CREATE TABLE IF NOT EXISTS workspace.cardpulse.silver_rejected_transactions (
  transaction_id STRING,
  card_number_masked STRING COMMENT 'Masked even here, only the last 4 digits are visible',
  merchant_name STRING,
  merchant_category STRING,
  amount STRING COMMENT 'Raw value from bronze',
  transaction_ts STRING COMMENT 'Raw value from bronze',
  reject_reason STRING COMMENT 'Every failed rule, separated by a semicolon',
  source_file STRING,
  run_id STRING,
  rejected_ts TIMESTAMP
)
USING ICEBERG
COMMENT 'Silver: rejected card transactions';
