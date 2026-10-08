from __future__ import annotations

from datetime import date

import pytest

from lakehouse import quality

COLS = "ticker string, date date, open double, high double, low double, close double, adj_close double, volume long"


def _rows(spark, rows):
    return spark.createDataFrame(rows, COLS)


GOOD = ("AAA", date(2024, 1, 2), 10.0, 11.0, 9.0, 10.5, 9.0, 100)


def test_clean_row_breaks_nothing(spark):
    clean, flagged = quality.split(_rows(spark, [GOOD]), quality.PRICE_RULES)
    assert clean.count() == 1 and flagged.count() == 0


@pytest.mark.parametrize(
    "row, rule",
    [
        ((None, date(2024, 1, 2), 10.0, 11.0, 9.0, 10.5, 9.0, 100), "ticker_not_null"),
        (("AAA", date(2024, 1, 2), 10.0, 11.0, 9.0, 0.0, 9.0, 100), "close_positive"),
        (("AAA", date(2024, 1, 2), -1.0, 11.0, 9.0, 10.5, 9.0, 100), "open_positive"),
        (("AAA", date(2024, 1, 2), 10.0, 11.0, 9.0, 10.5, None, 100), "adj_close_not_null"),
        (("AAA", date(2024, 1, 2), 10.0, 11.0, 9.0, 10.5, 9.0, -5), "volume_non_negative"),
    ],
)
def test_drop_rules_keep_bad_rows_out_of_silver(spark, row, rule):
    clean, flagged = quality.split(_rows(spark, [GOOD, row]), quality.PRICE_RULES)
    assert clean.count() == 1
    assert rule in flagged.collect()[0][quality.FAILED_RULES_COL]


def test_ohlc_violation_is_flagged_but_kept(spark):
    bad = ("AAA", date(2024, 1, 3), 10.0, 11.0, 10.25, 10.5, 9.0, 100)  # low > open
    clean, flagged = quality.split(_rows(spark, [GOOD, bad]), quality.PRICE_RULES)
    assert clean.count() == 2
    assert flagged.collect()[0][quality.FAILED_RULES_COL] == ["low_le_open"]


def test_null_open_only_fails_rules_that_check_for_null(spark):
    row = ("AAA", date(2024, 1, 2), None, 11.0, 9.0, 10.5, 9.0, 100)
    _, flagged = quality.split(_rows(spark, [row]), quality.PRICE_RULES)
    assert flagged.count() == 0  # as in the original: open > 0 ignores NULLs


def test_rule_counts_count_each_rule_a_row_breaks(spark):
    both = ("AAA", date(2024, 1, 2), 10.0, 8.0, 9.0, 10.5, 9.0, 100)  # high < low, open, close
    _, flagged = quality.split(_rows(spark, [both]), quality.PRICE_RULES)
    counts = quality.rule_failure_counts(flagged, quality.PRICE_RULES)
    assert counts["high_ge_low"] == counts["high_ge_open"] == counts["high_ge_close"] == 1
    assert counts["close_positive"] == 0


def test_quarantine_record_keeps_the_whole_row_as_json(spark):
    bad = ("AAA", date(2024, 1, 2), 10.0, 11.0, 9.0, 0.0, 9.0, 100)
    _, flagged = quality.split(_rows(spark, [bad]), quality.PRICE_RULES)
    rec = quality.quarantine_records(flagged, "prices_yfinance", quality.PRICE_RULES, "run1", "2024-01-02").collect()[0]
    assert rec["severity"] == "drop" and rec["dataset"] == "prices_yfinance"
    assert '"close":0.0' in rec["record"] and rec["run_date"] == date(2024, 1, 2)


def test_gate_tolerates_one_bad_print_in_ten_thousand():
    rules = quality.PRICE_RULES
    assert all(g.passed for g in quality.evaluate_gate({"low_le_open": 1}, 10_000, rules))
    failed = [g.rule for g in quality.evaluate_gate({"low_le_open": 2}, 10_000, rules) if not g.passed]
    assert failed == ["low_le_open"]


def test_gate_has_zero_tolerance_for_drop_rules_by_default():
    gates = quality.evaluate_gate({"close_positive": 1}, 1_000_000, quality.PRICE_RULES)
    assert [g.rule for g in gates if not g.passed] == ["close_positive"]
    relaxed = quality.evaluate_gate({"close_positive": 1}, 1_000_000, quality.PRICE_RULES, max_dropped=1)
    assert all(g.passed for g in relaxed)


def test_fred_range_check_skips_cpi(spark):
    df = spark.createDataFrame(
        [(date(2024, 1, 1), "CPIAUCSL", 308.4), (date(2024, 1, 2), "DGS10", 40.0)],
        "date date, series_id string, value double",
    )
    _, flagged = quality.split(df, quality.FRED_RULES)
    assert [r["series_id"] for r in flagged.collect()] == ["DGS10"]


def test_unknown_severity_rejected():
    with pytest.raises(ValueError):
        quality.Rule("x", "true", "fatal")
