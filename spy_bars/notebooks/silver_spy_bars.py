# Databricks notebook source
"""Quality rules shared by the silver job and local pytest tests."""
from __future__ import annotations

from datetime import datetime, time, timezone
import math
from typing import Any
from zoneinfo import ZoneInfo

REQUIRED_FIELDS = ("symbol", "ts", "open", "high", "low", "close", "volume")
NAMESPACE = "workspace.uc1_spy_bars"


def quality_reason(bar: dict[str, Any]) -> str:
    """Return all failures; timestamps from Spark without a zone represent UTC."""
    reasons = [f"missing_{field}" for field in REQUIRED_FIELDS if bar.get(field) is None]
    prices = [bar.get(field) for field in ("open", "high", "low", "close")]
    if all(value is not None for value in prices):
        opening, high, low, closing = prices
        if not all(math.isfinite(value) for value in prices):
            reasons.append("nonfinite_price")
        else:
            if high < low:
                reasons.append("high_below_low")
            if not low <= opening <= high:
                reasons.append("open_out_of_range")
            if not low <= closing <= high:
                reasons.append("close_out_of_range")
    if bar.get("volume") is not None and bar["volume"] < 0:
        reasons.append("negative_volume")
    timestamp = bar.get("ts")
    if timestamp is not None:
        timestamp = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp
        local = timestamp.astimezone(ZoneInfo("America/New_York"))
        if local.weekday() >= 5 or not time(9, 30) <= local.time() < time(16):
            reasons.append("outside_regular_hours")
    return ";".join(reasons)


def dedupe_order(bar: dict[str, Any]) -> tuple[str, ...]:
    """Latest ingestion wins; filename and payload break ties deterministically."""
    return tuple(str(bar.get(field) or "") for field in
                 ("ingestion_timestamp", "source_file_name", "_id", "tradeDate", *REQUIRED_FIELDS))


def deduplicate(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the latest copy per symbol/minute, including invalid corrections."""
    latest: dict[tuple[Any, Any], dict[str, Any]] = {}
    for bar in bars:
        key = (bar.get("symbol"), bar.get("ts"))
        if key not in latest or dedupe_order(bar) > dedupe_order(latest[key]):
            latest[key] = bar
    return list(latest.values())


def load_silver() -> None:
    """Quarantine every invalid source row and merge the latest valid bars."""
    from pyspark.sql import SparkSession, functions as functions
    from pyspark.sql.types import ArrayType, StringType
    from pyspark.sql.window import Window

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    bronze = f"{NAMESPACE}.bronze_spy_bars_1m"
    silver = f"{NAMESPACE}.silver_spy_bars_1m"
    quarantine = f"{NAMESPACE}.quarantine_spy_bars_1m"
    table_format = spark.sql(f"DESCRIBE DETAIL {bronze}").first()["format"]
    if table_format not in {"iceberg", "delta"}:
        raise ValueError(f"Unsupported bronze format: {table_format}")
    source = spark.table(bronze)
    columns = source.columns
    reason_rule = functions.udf(lambda row: quality_reason(row.asDict()), StringType())
    order_rule = functions.udf(lambda row: dedupe_order(row.asDict()), ArrayType(StringType()))
    classified = source.withColumn("reason", reason_rule(functions.struct(*columns)))
    classified.createOrReplaceTempView("classified_bars")
    # A snapshot of all rejected bronze copies also preserves missing-key failures.
    spark.sql(f"CREATE OR REPLACE TABLE {quarantine} USING {table_format} AS "
              "SELECT * FROM classified_bars WHERE reason <> ''")
    ranked = classified.withColumn("_rank", functions.row_number().over(
        Window.partitionBy("symbol", "ts").orderBy(order_rule(functions.struct(*columns)).desc())))
    ranked.filter("_rank = 1").drop("_rank").createOrReplaceTempView("latest_bars")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {silver} USING {table_format} AS SELECT * FROM {bronze} WHERE false")
    if spark.sql(f"DESCRIBE DETAIL {silver}").first()["format"] != table_format:
        raise ValueError("Silver format must match bronze")
    updates = ", ".join(f"target.`{column}` = source.`{column}`" for column in columns)
    names = ", ".join(f"`{column}`" for column in columns)
    values = ", ".join(f"source.`{column}`" for column in columns)
    spark.sql(f"""MERGE INTO {silver} target USING latest_bars source
        ON target.symbol = source.symbol AND target.ts = source.ts
        WHEN MATCHED AND source.reason <> '' THEN DELETE
        WHEN MATCHED AND source.reason = '' THEN UPDATE SET {updates}
        WHEN NOT MATCHED AND source.reason = '' THEN INSERT ({names}) VALUES ({values})""")
    spark.sql(f"SELECT count(*) AS silver_rows FROM {silver}").show()


if __name__ == "__main__":
    load_silver()
