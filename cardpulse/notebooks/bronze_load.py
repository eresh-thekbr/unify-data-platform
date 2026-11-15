"""Bronze layer: load the daily transactions CSV as-is and add load metadata."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

BRONZE_TABLE = "workspace.cardpulse.bronze_card_transactions"

SOURCE_COLUMNS = (
    "transaction_id",
    "card_number",
    "merchant_name",
    "merchant_category",
    "amount",
    "transaction_ts",
)

# Every column is read as text so bronze keeps the file's values unchanged.
RAW_TRANSACTIONS_SCHEMA = StructType(
    [StructField(column_name, StringType(), nullable=True) for column_name in SOURCE_COLUMNS]
)


def read_raw_transactions(spark: SparkSession, source_path: str) -> DataFrame:
    """Read the CSV with every column as a string."""
    return spark.read.option("header", True).schema(RAW_TRANSACTIONS_SCHEMA).csv(source_path)


def add_load_metadata(raw_transactions_df: DataFrame, source_file: str, run_id: str) -> DataFrame:
    """Add the file name, load time and run id to each raw row."""
    return (
        raw_transactions_df.withColumn("source_file", F.lit(source_file))
        .withColumn("load_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


def count_bronze_rows_for_run(spark: SparkSession, run_id: str) -> int:
    """Count what this run actually wrote to bronze."""
    return spark.table(BRONZE_TABLE).where(F.col("run_id") == run_id).count()


def load_bronze(spark: SparkSession, source_path: str, source_file: str, run_id: str) -> int:
    """Append the CSV to bronze and return the number of rows written."""
    raw_transactions_df = read_raw_transactions(spark, source_path)
    bronze_transactions_df = add_load_metadata(raw_transactions_df, source_file, run_id)
    bronze_transactions_df.writeTo(BRONZE_TABLE).append()
    return count_bronze_rows_for_run(spark, run_id)
