# Databricks notebook source
# MAGIC %md
# MAGIC # CardPulse batch pipeline
# MAGIC Loads one daily transactions CSV from the landing Volume through bronze, silver and gold,
# MAGIC then records the run in `pipeline_run_audit`. Set the `source_file` widget to the CSV name.

# COMMAND ----------

from datetime import datetime, timezone
from pathlib import Path
import uuid

from bronze_load import load_bronze
from gold_aggregate import refresh_gold
from run_audit import (
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    ensure_rows_reconcile,
    is_file_already_loaded,
    write_audit_row,
)
from silver_transform import load_silver

LANDING_VOLUME_PATH = "/Volumes/workspace/cardpulse/landing"
SQL_DIRECTORY = Path.cwd().parent / "sql"

# COMMAND ----------


def split_sql_statements(sql_text: str) -> list[str]:
    """Drop comment lines and split the file into single statements on ';'."""
    code_lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    return [statement.strip() for statement in "\n".join(code_lines).split(";") if statement.strip()]


def create_tables(sql_directory: Path) -> None:
    """Run every DDL file in name order. Each statement is IF NOT EXISTS, so reruns are safe."""
    for sql_file in sorted(sql_directory.glob("*.sql")):
        for statement in split_sql_statements(sql_file.read_text()):
            spark.sql(statement)


def run_pipeline(source_file: str) -> None:
    """Run bronze, silver and gold for one file and audit the outcome either way."""
    run_id = str(uuid.uuid4())
    started_ts = datetime.now(timezone.utc)
    rows_read = rows_loaded = rows_rejected = None
    try:
        rows_read = load_bronze(spark, f"{LANDING_VOLUME_PATH}/{source_file}", source_file, run_id)
        rows_loaded, rows_rejected = load_silver(spark, run_id)
        ensure_rows_reconcile(rows_read, rows_loaded, rows_rejected)
        refresh_gold(spark)
    except Exception as pipeline_error:
        write_audit_row(
            spark,
            run_id=run_id,
            source_file=source_file,
            started_ts=started_ts,
            finished_ts=datetime.now(timezone.utc),
            rows_read=rows_read,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            status=STATUS_FAILED,
            error_message=str(pipeline_error),
        )
        raise
    write_audit_row(
        spark,
        run_id=run_id,
        source_file=source_file,
        started_ts=started_ts,
        finished_ts=datetime.now(timezone.utc),
        rows_read=rows_read,
        rows_loaded=rows_loaded,
        rows_rejected=rows_rejected,
        status=STATUS_SUCCEEDED,
    )
    print(f"Run {run_id}: read={rows_read} loaded={rows_loaded} rejected={rows_rejected}")


# COMMAND ----------

dbutils.widgets.text("source_file", "card_transactions_2026-11-14.csv")
source_file = dbutils.widgets.get("source_file")

spark.conf.set("spark.sql.session.timeZone", "UTC")
create_tables(SQL_DIRECTORY)

if is_file_already_loaded(spark, source_file):
    dbutils.notebook.exit(f"Skipped: {source_file} was already loaded successfully")

run_pipeline(source_file)
