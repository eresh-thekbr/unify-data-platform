-- Raw transactions exactly as they appear in the CSV, plus load metadata.
CREATE TABLE IF NOT EXISTS workspace.cardpulse.bronze_card_transactions (
  transaction_id STRING,
  card_number STRING,
  merchant_name STRING,
  merchant_category STRING,
  amount STRING,
  transaction_ts STRING,
  source_file STRING COMMENT 'CSV file name the row came from',
  load_ts TIMESTAMP COMMENT 'When the row was written to bronze',
  run_id STRING COMMENT 'Pipeline run that loaded the row'
)
USING ICEBERG
COMMENT 'Bronze: daily card transactions loaded as-is';
