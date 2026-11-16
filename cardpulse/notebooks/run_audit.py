"""Audit: record one row per pipeline run and guard against loading a file twice."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

AUDIT_TABLE = "workspace.cardpulse.pipeline_run_audit"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"
MAX_ERROR_MESSAGE_LENGTH = 2000

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("source_file", StringType()),
        StructField("started_ts", TimestampType()),
        StructField("finished_ts", TimestampType()),
        StructField("rows_read", LongType()),
        StructField("rows_loaded", LongType()),
        StructField("rows_rejected", LongType()),
        StructField("status", StringType()),
        StructField("error_message", StringType()),
    ]
)


def is_file_already_loaded(spark: SparkSession, source_file: str) -> bool:
    """True when an earlier run of this file succeeded."""
    succeeded_runs_df = spark.table(AUDIT_TABLE).where(
        (F.col("source_file") == source_file) & (F.col("status") == STATUS_SUCCEEDED)
    )
    return not succeeded_runs_df.isEmpty()


def ensure_rows_reconcile(rows_read: int, rows_loaded: int, rows_rejected: int) -> None:
    """Fail the run unless every bronze row went to silver or rejected."""
    if rows_read != rows_loaded + rows_rejected:
        raise ValueError(
            f"Row counts do not reconcile: rows_read={rows_read}, "
            f"rows_loaded={rows_loaded}, rows_rejected={rows_rejected}"
        )


def write_audit_row(
    spark: SparkSession,
    *,
    run_id: str,
    source_file: str,
    started_ts: datetime,
    finished_ts: datetime,
    rows_read: int | None,
    rows_loaded: int | None,
    rows_rejected: int | None,
    status: str,
    error_message: str | None = None,
) -> None:
    """Append one run's outcome. Counts are null for steps that never ran."""
    if error_message is not None:
        error_message = error_message[:MAX_ERROR_MESSAGE_LENGTH]
    audit_row = (
        run_id,
        source_file,
        started_ts,
        finished_ts,
        rows_read,
        rows_loaded,
        rows_rejected,
        status,
        error_message,
    )
    spark.createDataFrame([audit_row], AUDIT_SCHEMA).writeTo(AUDIT_TABLE).append()
