"""The landing contract: column names and types of the parquet files that
scripts/export_snapshot.py writes and the bronze layer reads.

Fixing the contract at the export step means a type surprise (a volume column
that is float64 in one file because of a NaN and int64 in another) is caught
before upload, not halfway through a streaming read.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def _schema(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


YFINANCE_PRICES = _schema(
    ("date", DateType()),
    ("ticker", StringType()),
    ("open", DoubleType()),
    ("high", DoubleType()),
    ("low", DoubleType()),
    ("close", DoubleType()),
    ("adj_close", DoubleType()),
    ("volume", LongType()),
    ("dividends", DoubleType()),
    ("stock_splits", DoubleType()),
)

TIINGO_PRICES = _schema(
    ("date", DateType()),
    ("ticker", StringType()),
    ("open", DoubleType()),
    ("high", DoubleType()),
    ("low", DoubleType()),
    ("close", DoubleType()),
    ("adj_close", DoubleType()),
    ("adj_open", DoubleType()),
    ("adj_high", DoubleType()),
    ("adj_low", DoubleType()),
    ("volume", LongType()),
    ("adj_volume", DoubleType()),
)

CONSTITUENTS = _schema(
    ("ticker", StringType()),
    ("company_name", StringType()),
    ("gics_sector", StringType()),
    ("gics_sub_industry", StringType()),
    ("date_added", DateType()),
    ("snapshot_date", DateType()),
)

OPENFIGI_MAPPING = _schema(
    ("ticker", StringType()),
    ("figi", StringType()),
    ("composite_figi", StringType()),
    ("share_class_figi", StringType()),
    ("name", StringType()),
    ("security_type", StringType()),
    ("market_sector", StringType()),
    ("snapshot_date", DateType()),
)

FRED_SERIES = _schema(
    ("date", DateType()),
    ("series_id", StringType()),
    ("value", DoubleType()),
)

LANDING_SCHEMAS: dict[str, StructType] = {
    "yfinance_prices": YFINANCE_PRICES,
    "tiingo_prices": TIINGO_PRICES,
    "constituents": CONSTITUENTS,
    "openfigi_mapping": OPENFIGI_MAPPING,
    "fred_series": FRED_SERIES,
}

# Bronze adds these to every source row.
BRONZE_METADATA_COLUMNS = ("_source_file", "_file_modified_at", "_drop_date", "_ingested_at")
