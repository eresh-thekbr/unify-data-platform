-- One row per pipeline run with its row counts and outcome.
CREATE TABLE IF NOT EXISTS workspace.cardpulse.pipeline_run_audit (
  run_id STRING,
  source_file STRING,
  started_ts TIMESTAMP,
  finished_ts TIMESTAMP,
  rows_read BIGINT COMMENT 'Rows written to bronze by this run',
  rows_loaded BIGINT COMMENT 'Rows written to silver by this run',
  rows_rejected BIGINT COMMENT 'Rows written to silver_rejected_transactions by this run',
  status STRING COMMENT 'SUCCEEDED or FAILED',
  error_message STRING
)
USING ICEBERG
COMMENT 'Audit log of CardPulse pipeline runs';
