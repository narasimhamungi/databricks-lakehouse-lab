from __future__ import annotations

import os
from datetime import date

import pytest
from pyspark.sql import functions as F

from lakehouse import bronze, export, silver
from lakehouse.schemas import TIINGO_PRICES, YFINANCE_PRICES
from tests import factory


@pytest.fixture
def landing(tmp_path):
    bronze_root = factory.write_mdl_bronze(str(tmp_path / "mdl"))
    root = str(tmp_path / "landing")
    export.export_snapshot(bronze_root, factory.INGEST_DATE, root)
    return root


def _read(spark, landing, source, schema):
    return bronze.with_ingest_metadata(
        spark.read.schema(schema).option("recursiveFileLookup", "true").parquet(os.path.join(landing, source))
    )


def test_metadata_traces_every_row_to_its_file_and_drop(spark, landing):
    df = _read(spark, landing, "yfinance_prices", YFINANCE_PRICES)
    drops = {r[0] for r in df.select("_drop_date").distinct().collect()}
    assert drops == set(factory.TRADING_DAYS[-3:])
    assert df.where(F.col("_source_file").isNull() | F.col("_drop_date").isNull()).count() == 0
    assert df.where(~F.col("_source_file").contains("drop_date=")).count() == 0


def test_file_outside_a_drop_directory_gets_null_drop_date(spark, tmp_path):
    path = str(tmp_path / "stray")
    spark.createDataFrame([(date(2024, 1, 2), "AAA")], "date date, ticker string").write.parquet(path)
    df = bronze.with_ingest_metadata(spark.read.parquet(path))
    assert df.where(F.col("_drop_date").isNull()).count() == 1


def test_batch_mode_skips_files_already_ingested(spark, landing):
    landed = _read(spark, landing, "tiingo_prices", TIINGO_PRICES)
    first_drop = landed.where(F.col("_drop_date") == F.lit(factory.TRADING_DAYS[-3]))
    remaining = bronze.new_files_only(landed, first_drop)
    assert remaining.count() == landed.count() - first_drop.count()
    assert bronze.new_files_only(landed, landed).count() == 0


def test_type_prices_casts_and_strips_time(spark):
    df = spark.createDataFrame(
        [("2024-01-02 15:30:00", " AAA ", "10", "11", "9", "10.5", "9.45", "100")],
        "date string, ticker string, open string, high string, low string, close string, "
        "adj_close string, volume string",
    )
    row = silver.type_prices(df).collect()[0]
    assert row["date"] == date(2024, 1, 2) and row["ticker"] == "AAA"
    assert isinstance(row["close"], float) and isinstance(row["volume"], int)


def test_dedupe_keeps_the_row_from_the_latest_drop(spark):
    df = spark.createDataFrame(
        [
            ("AAA", date(2024, 1, 9), 10.0, date(2024, 1, 9), "f1"),
            ("AAA", date(2024, 1, 9), 11.0, date(2024, 1, 10), "f2"),
            ("AAA", date(2024, 1, 10), 12.0, date(2024, 1, 10), "f2"),
            ("AAA", date(2024, 1, 10), 12.0, date(2024, 1, 10), "f2"),
        ],
        "ticker string, date date, close double, _drop_date date, _source_file string",
    )
    out = {(r["date"], r["close"]) for r in silver.dedupe_latest(df, silver.PRICE_KEYS).collect()}
    assert out == {(date(2024, 1, 9), 11.0), (date(2024, 1, 10), 12.0)}


def test_yfinance_adjusted_ohlc_derived_from_close_factor(spark):
    df = spark.createDataFrame(
        [(10.0, 12.0, 8.0, 11.0, 5.5, 100)],
        "open double, high double, low double, close double, adj_close double, volume long",
    )
    r = silver.adjust_prices(df).collect()[0]
    assert (r["adj_open"], r["adj_high"], r["adj_low"]) == pytest.approx((5.0, 6.0, 4.0))
    assert r["adj_volume"] == 100.0  # left as reported, as in the original


def test_tiingo_native_adjusted_values_are_kept(spark):
    df = spark.createDataFrame(
        [(10.0, 11.0, 5.5, 4.0, 6.5, 3.0, 50.0, 100)],
        "open double, close double, adj_close double, adj_open double, adj_high double, "
        "adj_low double, adj_volume double, volume long",
    )
    r = silver.adjust_prices(df).collect()[0]
    assert (r["adj_open"], r["adj_high"], r["adj_low"], r["adj_volume"]) == (4.0, 6.5, 3.0, 50.0)


def test_adjusting_with_a_non_positive_close_is_refused(spark):
    df = spark.createDataFrame([(0.0,)], "close double")
    with pytest.raises(ValueError, match="close <= 0"):
        silver.assert_positive_close(df)


def test_silver_price_columns_are_identical_for_both_sources(spark, landing):
    shapes = []
    for source, schema in (("yfinance_prices", YFINANCE_PRICES), ("tiingo_prices", TIINGO_PRICES)):
        df = silver.to_silver_prices(silver.adjust_prices(silver.type_prices(_read(spark, landing, source, schema))))
        shapes.append([(f.name, f.dataType.simpleString()) for f in df.schema])
    assert shapes[0] == shapes[1]


def test_silver_reference_drops_bookkeeping_and_rescued_data(spark):
    df = spark.createDataFrame(
        [("AAA", date(2024, 1, 12), date(2024, 1, 9), "f1", None, None, '{"x":1}')],
        "ticker string, snapshot_date date, _drop_date date, _source_file string, "
        "_file_modified_at timestamp, _ingested_at timestamp, _rescued_data string",
    )
    out = silver.silver_reference(df, ("ticker", "snapshot_date"))
    assert out.columns == ["ticker", "snapshot_date", "_drop_date"]
