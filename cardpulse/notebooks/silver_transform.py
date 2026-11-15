"""Silver layer: validate, deduplicate, mask and standardize one run's bronze rows.

Every bronze row of a run lands in exactly one place: silver_card_transactions or
silver_rejected_transactions.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from bronze_load import BRONZE_TABLE

SILVER_TABLE = "workspace.cardpulse.silver_card_transactions"
REJECTED_TABLE = "workspace.cardpulse.silver_rejected_transactions"

# Source timestamps without a zone are treated as UTC (the session time zone).
SOURCE_TIMESTAMP_FORMATS = ("yyyy-MM-dd'T'HH:mm:ssX", "MM/dd/yyyy HH:mm:ss")
CARD_NUMBER_PATTERN = r"^[0-9]{16}$"
CARD_NUMBER_MASK = "************"
DUPLICATE_REASON = "duplicate_transaction_id"

SILVER_COLUMNS = (
    "transaction_id",
    "card_number_masked",
    "merchant_name",
    "merchant_category",
    "amount",
    "transaction_ts",
    "transaction_date",
    "source_file",
    "run_id",
    "processed_ts",
)

REJECTED_COLUMNS = (
    "transaction_id",
    "card_number_masked",
    "merchant_name",
    "merchant_category",
    "amount",
    "transaction_ts",
    "reject_reason",
    "source_file",
    "run_id",
    "rejected_ts",
)


def read_bronze_rows_for_run(spark: SparkSession, run_id: str) -> DataFrame:
    """Return only the bronze rows this run loaded."""
    return spark.table(BRONZE_TABLE).where(F.col("run_id") == run_id)


def is_blank(column: Column) -> Column:
    """True when the value is null or only whitespace."""
    return column.isNull() | (F.trim(column) == "")


def parse_transaction_ts(raw_ts_column: Column) -> Column:
    """Try each known source format and return the first that parses, else null."""
    parsed_candidates = [
        F.try_to_timestamp(F.trim(raw_ts_column), F.lit(timestamp_format))
        for timestamp_format in SOURCE_TIMESTAMP_FORMATS
    ]
    return F.coalesce(*parsed_candidates)


def mask_card_number(card_number_column: Column) -> Column:
    """Keep only the last 4 characters of the card number visible."""
    trimmed_card_number = F.trim(card_number_column)
    return F.when(
        ~is_blank(card_number_column),
        F.concat(F.lit(CARD_NUMBER_MASK), F.substring(trimmed_card_number, -4, 4)),
    )


def add_parsed_columns(bronze_rows_df: DataFrame) -> DataFrame:
    """Add typed versions of amount and timestamp; null means the value could not be parsed."""
    return bronze_rows_df.withColumn(
        "parsed_amount", F.expr("try_cast(trim(amount) AS DECIMAL(12, 2))")
    ).withColumn("parsed_transaction_ts", parse_transaction_ts(F.col("transaction_ts")))


def build_reject_reason() -> Column:
    """Join the name of every failed rule with ';'. An empty string means the row is valid."""
    card_number = F.col("card_number")
    failed_rules = {
        "missing_transaction_id": is_blank(F.col("transaction_id")),
        "missing_card_number": is_blank(card_number),
        "invalid_card_number": ~is_blank(card_number) & ~F.trim(card_number).rlike(CARD_NUMBER_PATTERN),
        "missing_merchant_category": is_blank(F.col("merchant_category")),
        "invalid_amount": F.col("parsed_amount").isNull(),
        "negative_amount": F.col("parsed_amount") < 0,
        "invalid_transaction_ts": F.col("parsed_transaction_ts").isNull(),
    }
    return F.concat_ws(
        ";", *[F.when(rule_failed, F.lit(rule_name)) for rule_name, rule_failed in failed_rules.items()]
    )


def add_duplicate_rank(valid_rows_df: DataFrame) -> DataFrame:
    """Number the copies of each transaction_id; copy 1 is kept, the rest are duplicates."""
    copies_of_same_transaction = Window.partitionBy("transaction_id").orderBy(
        "parsed_transaction_ts", "load_ts"
    )
    return valid_rows_df.withColumn("duplicate_rank", F.row_number().over(copies_of_same_transaction))


def to_silver_rows(kept_rows_df: DataFrame) -> DataFrame:
    """Shape valid rows into the silver table's columns."""
    return kept_rows_df.select(
        F.trim("transaction_id").alias("transaction_id"),
        mask_card_number(F.col("card_number")).alias("card_number_masked"),
        F.trim("merchant_name").alias("merchant_name"),
        F.lower(F.trim("merchant_category")).alias("merchant_category"),
        F.col("parsed_amount").alias("amount"),
        F.col("parsed_transaction_ts").alias("transaction_ts"),
        F.to_date("parsed_transaction_ts").alias("transaction_date"),
        "source_file",
        "run_id",
        F.current_timestamp().alias("processed_ts"),
    )


def to_rejected_rows(rejected_rows_df: DataFrame) -> DataFrame:
    """Shape rejected rows into the rejected table's columns, keeping raw values except the card."""
    return rejected_rows_df.select(
        "transaction_id",
        mask_card_number(F.col("card_number")).alias("card_number_masked"),
        "merchant_name",
        "merchant_category",
        "amount",
        "transaction_ts",
        "reject_reason",
        "source_file",
        "run_id",
        F.current_timestamp().alias("rejected_ts"),
    )


def split_silver_and_rejected(bronze_rows_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (silver rows, rejected rows) for one run's bronze rows."""
    checked_rows_df = add_parsed_columns(bronze_rows_df).withColumn("reject_reason", build_reject_reason())
    invalid_rows_df = checked_rows_df.where(F.col("reject_reason") != "")
    ranked_valid_rows_df = add_duplicate_rank(checked_rows_df.where(F.col("reject_reason") == ""))

    kept_rows_df = ranked_valid_rows_df.where(F.col("duplicate_rank") == 1)
    duplicate_rows_df = ranked_valid_rows_df.where(F.col("duplicate_rank") > 1).withColumn(
        "reject_reason", F.lit(DUPLICATE_REASON)
    )

    silver_rows_df = to_silver_rows(kept_rows_df)
    rejected_rows_df = to_rejected_rows(invalid_rows_df).unionByName(to_rejected_rows(duplicate_rows_df))
    return silver_rows_df, rejected_rows_df


def count_rows_for_run(spark: SparkSession, table_name: str, run_id: str) -> int:
    """Count what this run actually wrote to a table."""
    return spark.table(table_name).where(F.col("run_id") == run_id).count()


def load_silver(spark: SparkSession, run_id: str) -> tuple[int, int]:
    """Write this run's silver and rejected rows; return (loaded row count, rejected row count)."""
    bronze_rows_df = read_bronze_rows_for_run(spark, run_id)
    silver_rows_df, rejected_rows_df = split_silver_and_rejected(bronze_rows_df)
    silver_rows_df.writeTo(SILVER_TABLE).append()
    rejected_rows_df.writeTo(REJECTED_TABLE).append()

    loaded_row_count = count_rows_for_run(spark, SILVER_TABLE, run_id)
    rejected_row_count = count_rows_for_run(spark, REJECTED_TABLE, run_id)
    return loaded_row_count, rejected_row_count
