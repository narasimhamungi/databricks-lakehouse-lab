"""Silver: typed, deduplicated, fully adjusted OHLCV, one table per price
source, plus clean reference tables (constituents, FIGI map, FRED).

Ported from marketdata-lakehouse src/transform/silver_prices.py. The two
price sources stay separate on purpose: reconciliation compares them, so
blending them here would destroy what it needs to check.

Differences from the pandas original, all forced by incremental loading:
- Bronze holds several drops, so dedupe keeps the row from the latest drop
  (then the latest file) instead of "last row in one partition".
- Rows carry _drop_date into silver, and the MERGE only lets a newer drop
  overwrite an older one.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PRICE_KEYS = ("ticker", "date")
PRICE_VALUE_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "adj_open",
    "adj_high",
    "adj_low",
    "volume",
    "adj_volume",
)
SILVER_PRICE_COLUMNS = ("ticker", "date", *PRICE_VALUE_COLUMNS, "_drop_date", "_source_file")
NATIVE_ADJUSTED = ("adj_open", "adj_high", "adj_low", "adj_volume")


def type_prices(df: DataFrame) -> DataFrame:
    """Cast to the silver types. Daily bars, so any time component is dropped."""
    out = df.withColumn("date", F.to_date("date")).withColumn("ticker", F.trim(F.col("ticker").cast("string")))
    for c in ("open", "high", "low", "close", "adj_close", "adj_open", "adj_high", "adj_low", "adj_volume"):
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("double"))
    if "volume" in out.columns:
        out = out.withColumn("volume", F.col("volume").cast("long"))
    return out


def dedupe_latest(df: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    """One row per key: the one from the latest drop, then the latest file."""
    order = [F.col("_drop_date").desc_nulls_last(), F.col("_source_file").desc_nulls_last()]
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def has_native_adjusted(df: DataFrame) -> bool:
    """Tiingo bronze carries its own adjusted OHLC; yfinance only adj_close.
    Decided by column presence, as in the original, not by source name."""
    return set(NATIVE_ADJUSTED).issubset(df.columns)


def adjust_prices(df: DataFrame) -> DataFrame:
    """Use source-native adjusted OHLC when present; otherwise apply the day's
    adj_close/close factor to open/high/low. Volume is left as reported, not
    split-adjusted (a known limitation carried over from the original).

    Callers must have removed close <= 0 rows first (the close_positive
    quality rule); this raises rather than divide by a bad close.
    """
    if has_native_adjusted(df):
        return df
    factor = F.col("adj_close") / F.col("close")
    return (
        df.withColumn("adj_open", F.col("open") * factor)
        .withColumn("adj_high", F.col("high") * factor)
        .withColumn("adj_low", F.col("low") * factor)
        .withColumn("adj_volume", F.col("volume").cast("double"))
    )


def assert_positive_close(df: DataFrame) -> None:
    bad = df.where(F.col("close") <= 0).limit(1).count()
    if bad:
        raise ValueError(
            "Cannot derive adjustment factor: close <= 0 reached adjust_prices; quality split must run first"
        )


def to_silver_prices(df: DataFrame) -> DataFrame:
    return df.select(*[F.col(c) for c in SILVER_PRICE_COLUMNS])


def silver_reference(df: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    """Constituents, FIGI map and FRED: dedupe on the natural key; bronze
    bookkeeping columns other than _drop_date are dropped, including Auto
    Loader's _rescued_data (no-op when absent)."""
    return dedupe_latest(df, keys).drop("_source_file", "_file_modified_at", "_ingested_at", "_rescued_data")
