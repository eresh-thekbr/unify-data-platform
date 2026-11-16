"""Gold layer: daily spend per merchant category, rebuilt from all of silver."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver_transform import SILVER_TABLE

GOLD_TABLE = "workspace.cardpulse.gold_daily_spend_by_category"


def build_daily_spend_by_category(silver_transactions_df: DataFrame) -> DataFrame:
    """Sum and average spend for each day and category."""
    return (
        silver_transactions_df.groupBy("transaction_date", "merchant_category")
        .agg(
            F.count(F.lit(1)).alias("transaction_count"),
            F.sum("amount").cast("decimal(18, 2)").alias("total_spend_amount"),
            F.round(F.avg("amount"), 2).cast("decimal(12, 2)").alias("average_spend_amount"),
        )
        .withColumn("refreshed_ts", F.current_timestamp())
    )


def overwrite_gold_table(spark: SparkSession, daily_spend_df: DataFrame) -> None:
    """Replace the table's rows; the table itself, and its Iceberg format, stay in place."""
    gold_column_order = spark.table(GOLD_TABLE).columns
    daily_spend_df.select(*gold_column_order).write.insertInto(GOLD_TABLE, overwrite=True)


def refresh_gold(spark: SparkSession) -> None:
    """Rebuild gold from the current contents of silver."""
    daily_spend_df = build_daily_spend_by_category(spark.table(SILVER_TABLE))
    overwrite_gold_table(spark, daily_spend_df)
