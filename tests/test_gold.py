from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from lakehouse import gold

DIM_COLS = (
    "security_key bigint, ticker string, company_name string, gics_sector string, "
    "gics_sub_industry string, figi string, effective_from date, effective_to date, is_current boolean"
)
SNAP_COLS = "ticker string, company_name string, gics_sector string, gics_sub_industry string, figi string"


# --- dim_date ---------------------------------------------------------------


def test_dim_date_calendar_fields(spark):
    traded = spark.createDataFrame([(date(2024, 1, 2),)], "date date")
    rows = {r["date"]: r for r in gold.build_dim_date(spark, "2024-01-01", "2024-01-07", traded).collect()}
    assert len(rows) == 7
    mon, sun = rows[date(2024, 1, 1)], rows[date(2024, 1, 7)]
    assert (mon["day_of_week"], mon["day_name"], mon["is_weekday"]) == (0, "Monday", True)
    assert (sun["day_of_week"], sun["day_name"], sun["is_weekday"]) == (6, "Sunday", False)
    assert mon["date_key"] == 20240101 and mon["quarter"] == 1 and mon["month_name"] == "January"


def test_trading_day_comes_from_the_data_not_a_calendar(spark):
    # 2024-01-01 is a weekday market holiday: no price rows, so not a trading day.
    traded = spark.createDataFrame([(date(2024, 1, 2),), (date(2024, 1, 2),)], "date date")
    rows = {
        r["date"]: r["is_trading_day"] for r in gold.build_dim_date(spark, "2024-01-01", "2024-01-03", traded).collect()
    }
    assert rows == {date(2024, 1, 1): False, date(2024, 1, 2): True, date(2024, 1, 3): False}


def test_dim_date_rejects_inverted_range(spark):
    with pytest.raises(ValueError):
        gold.build_dim_date(spark, "2024-02-01", "2024-01-01", spark.createDataFrame([], "date date"))


# --- dim_security SCD-2 ---------------------------------------------------------


def _current(spark, rows):
    return spark.createDataFrame(
        [(gold_key(t), t, c, s, si, f, date(2024, 1, 1), None, True) for t, c, s, si, f in rows], DIM_COLS
    )


def gold_key(t):
    return abs(hash(t)) % 10_000


def test_scd2_classifies_new_changed_removed_unchanged(spark):
    current = _current(
        spark,
        [
            ("AAA", "Alpha", "IT", "Software", "F1"),
            ("BBB", "Beta", "Financials", "Banks", "F2"),
            ("OLD", "Gone", "Energy", "Oil", "F9"),
        ],
    )
    snap = spark.createDataFrame(
        [
            ("AAA", "Alpha", "IT", "Software", "F1"),
            ("BBB", "Beta", "Industrials", "Banks", "F2"),
            ("NEW", "Newco", "IT", "Chips", None),
        ],
        SNAP_COLS,
    )
    classified = gold.scd2_classify(current, snap)
    status = {r["ticker"]: r["status"] for r in classified.collect()}
    assert status == {"AAA": "unchanged", "BBB": "changed", "OLD": "removed", "NEW": "new"}
    assert gold.scd2_summary(classified) == {"new": 1, "changed": 1, "closed": 1, "unchanged": 1}


def test_scd2_null_figi_that_stays_null_is_not_a_change(spark):
    current = _current(spark, [("CCC", "Gamma", "Utilities", "Electric", None)])
    same = spark.createDataFrame([("CCC", "Gamma", "Utilities", "Electric", None)], SNAP_COLS)
    filled = spark.createDataFrame([("CCC", "Gamma", "Utilities", "Electric", "F3")], SNAP_COLS)
    assert gold.scd2_classify(current, same).collect()[0]["status"] == "unchanged"
    assert gold.scd2_classify(current, filled).collect()[0]["status"] == "changed"


def test_scd2_stage_closes_and_reopens_changed_rows(spark):
    current = _current(spark, [("BBB", "Beta", "Financials", "Banks", "F2"), ("OLD", "Gone", "Energy", "Oil", "F9")])
    snap = spark.createDataFrame([("BBB", "Beta", "Industrials", "Banks", "F2")], SNAP_COLS)
    stage = gold.scd2_stage(gold.scd2_classify(current, snap), "2024-02-01").collect()
    actions = sorted((r["ticker"], r["action"], r["merge_key"]) for r in stage)
    assert actions == [("BBB", "close", "BBB"), ("BBB", "insert", None), ("OLD", "close", "OLD")]
    ins = [r for r in stage if r["action"] == "insert"][0]
    assert ins["gics_sector"] == "Industrials" and ins["snapshot_date"] == date(2024, 2, 1)


def test_security_key_is_deterministic_per_ticker_and_version(spark):
    df = spark.createDataFrame(
        [("AAA", date(2024, 1, 1)), ("AAA", date(2024, 1, 1)), ("AAA", date(2024, 2, 1))], "ticker string, d date"
    )
    keys = [r[0] for r in df.select(gold.security_key(F.col("ticker"), F.col("d"))).collect()]
    assert keys[0] == keys[1] != keys[2]


def test_snapshot_joins_latest_figi_and_keeps_unmapped_tickers(spark):
    cons = spark.createDataFrame(
        [
            ("AAA", "Alpha", "IT", "Software", date(2024, 2, 1)),
            ("CCC", "Gamma", "Utilities", "Electric", date(2024, 2, 1)),
        ],
        "ticker string, company_name string, gics_sector string, gics_sub_industry string, snapshot_date date",
    )
    figi = spark.createDataFrame(
        [("AAA", "OLDF", date(2024, 1, 1)), ("AAA", "NEWF", date(2024, 2, 1)), ("AAA", "FUTURE", date(2024, 3, 1))],
        "ticker string, figi string, snapshot_date date",
    )
    rows = {r["ticker"]: r["figi"] for r in gold.snapshot_with_figi(cons, figi, "2024-02-01").collect()}
    assert rows == {"AAA": "NEWF", "CCC": None}


def test_only_snapshots_newer_than_the_dimension_are_applied():
    dates = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), None]
    assert gold.snapshots_to_apply(dates, None) == ["2024-01-01", "2024-02-01", "2024-03-01"]
    assert gold.snapshots_to_apply(dates, date(2024, 2, 1)) == ["2024-03-01"]
    assert gold.snapshots_to_apply(dates, date(2024, 3, 1)) == []


# --- reconciliation and facts ------------------------------------------------------

PX = "ticker string, date date, adj_close double"


def test_reconcile_flags_match_the_original_rules(spark):
    d1, d2, d3 = date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)
    yf = spark.createDataFrame([("AAA", d1, 100.0), ("BBB", d1, 100.0), ("CCC", d1, 50.0), ("EDGE", d2, 100.0)], PX)
    tg = spark.createDataFrame([("AAA", d1, 100.2), ("BBB", d1, 101.0), ("DDD", d3, 10.0), ("EDGE", d2, 100.5)], PX)
    out = {r["ticker"]: r for r in gold.reconcile(yf, tg).collect()}
    assert out["AAA"]["reconciliation_flag"] == "agreed"
    assert out["BBB"]["reconciliation_flag"] == "disagreed"
    assert out["BBB"]["pct_diff"] == pytest.approx(1.0 / 100.5)  # against the mean, not either source
    assert out["CCC"]["reconciliation_flag"] == "single_source" and out["CCC"]["pct_diff"] is None
    assert (out["DDD"]["primary_source"], out["DDD"]["sources_available"]) == ("tiingo", ["tiingo"])
    assert out["AAA"]["primary_source"] == "yfinance" and out["AAA"]["adj_close"] == 100.0
    # 0.5 / 100.25 = 0.4988% sits under the 0.5% tolerance
    assert out["EDGE"]["reconciliation_flag"] == "agreed"


def test_reconciliation_counts_mirror_the_report_headline(spark):
    d = date(2024, 1, 2)
    yf = spark.createDataFrame([("AAA", d, 100.0), ("BBB", d, 100.0), ("CCC", d, 50.0)], PX)
    tg = spark.createDataFrame([("AAA", d, 100.0), ("BBB", d, 110.0), ("DDD", d, 10.0)], PX)
    assert gold.reconciliation_counts(gold.reconcile(yf, tg)) == {
        "compared": 2,
        "flagged": 1,
        "yfinance_only": 1,
        "tiingo_only": 1,
    }


def test_unmatched_keys_are_counted_not_silently_dropped(spark):
    d = date(2024, 1, 2)
    prices = spark.createDataFrame([("AAA", d, 1.0), ("ZZZ", d, 1.0), ("AAA", date(2030, 1, 1), 1.0)], PX)
    dim_sec = spark.createDataFrame([(1, "AAA")], "security_key bigint, ticker string")
    dim_date = spark.createDataFrame([(20240102, d)], "date_key int, date date")
    keyed = gold.attach_keys(prices, dim_sec, dim_date)
    assert gold.key_match_counts(keyed) == {"rows": 3, "skipped_no_security_key": 1, "skipped_no_date_key": 1}
    assert gold.matched(keyed).count() == 1


def test_fact_price_rows_use_postgres_numeric_types(spark):
    keyed = spark.createDataFrame(
        [(1, 20240102, 1.1234567, 2.0, 1.0, 1.5, 1.0, 2.0, 1.0, 1.5, 100, 99.9)],
        "security_key bigint, date_key int, open double, high double, low double, close double, adj_open double, "
        "adj_high double, adj_low double, adj_close double, volume long, adj_volume double",
    )
    df = gold.fact_price_daily_rows(keyed, "tiingo")
    types = dict((f.name, f.dataType.simpleString()) for f in df.schema)
    assert types["open"] == "decimal(18,6)" and types["adj_volume"] == "bigint"
    r = df.collect()[0]
    assert r["source"] == "tiingo" and str(r["open"]) == "1.123457" and r["adj_volume"] == 99


def test_macro_rows_without_a_date_key_are_returned_separately(spark):
    fred = spark.createDataFrame(
        [(date(2024, 1, 2), "DGS10", 4.0), (date(2018, 1, 1), "DGS10", 2.0)],
        "date date, series_id string, value double",
    )
    dim_date = spark.createDataFrame([(20240102, date(2024, 1, 2))], "date_key int, date date")
    rows, unmatched = gold.fact_macro_rate_rows(fred, dim_date)
    assert rows.count() == 1 and unmatched.count() == 1
