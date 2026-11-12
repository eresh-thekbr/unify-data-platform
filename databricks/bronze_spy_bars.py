# Databricks notebook source
"""Append explicitly typed SPY JSON bars to a managed Iceberg bronze table."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

SOURCE_PATH = "/Volumes/workspace/uc1_spy_bars/spy_bars_landing"
CHECKPOINT_PATH = "/Volumes/workspace/uc1_spy_bars/spy_bars_checkpoints/bronze_spy_bars_1m"
TABLE_NAME = "workspace.uc1_spy_bars.bronze_spy_bars_1m"


def bar_schema() -> StructType:
    """Declare every source field; never infer market bar types."""
    return StructType([
        StructField("symbol", StringType()),
        StructField("ts", TimestampType()),
        StructField("tradeDate", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
        StructField("_id", StringType()),
    ])


def read_bars(spark: SparkSession) -> DataFrame:
    """Discover completed JSON Lines files and attach source and ingestion audits."""
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .option("pathGlobFilter", "*.jsonl")
            .option("mode", "FAILFAST")
            .schema(bar_schema())
            .load(SOURCE_PATH)
            .select("*", col("_metadata.file_name").alias("source_file_name"))
            .withColumn("ingestion_timestamp", current_timestamp()))


def load_bronze(spark: SparkSession) -> None:
    """Create the Iceberg target and append all currently available files."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            symbol STRING, ts TIMESTAMP, tradeDate STRING,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, _id STRING,
            source_file_name STRING, ingestion_timestamp TIMESTAMP
        ) USING ICEBERG
    """)
    table_details = spark.sql(f"DESCRIBE DETAIL {TABLE_NAME}").first()
    if table_details is None or table_details["format"] != "iceberg":
        raise ValueError("Bronze target must be a managed Iceberg table; refusing a format fallback")
    query = (read_bars(spark).writeStream
             .format("iceberg")
             .outputMode("append")
             .option("checkpointLocation", CHECKPOINT_PATH)
             .trigger(availableNow=True)
             .toTable(TABLE_NAME))
    query.awaitTermination()
    spark.sql(f"""SELECT COUNT(*) AS row_count, MIN(ts) AS first_ts,
                  MAX(ts) AS last_ts FROM {TABLE_NAME}""").show(truncate=False)


load_bronze(SparkSession.builder.getOrCreate())
