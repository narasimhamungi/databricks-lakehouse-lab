from __future__ import annotations

import json
import os
from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest

from lakehouse import export, manifest
from tests import factory


@pytest.fixture
def exported(tmp_path):
    bronze = factory.write_mdl_bronze(str(tmp_path / "mdl"))
    report = factory.write_recon_report(str(tmp_path / "recon.txt"))
    landing = str(tmp_path / "landing")
    summary = export.export_snapshot(bronze, factory.INGEST_DATE, landing, reconciliation_report=report)
    return landing, summary


def test_drops_are_the_last_three_trading_days(exported):
    _, summary = exported
    assert summary["drop_dates"] == [d.isoformat() for d in factory.TRADING_DAYS[-3:]]


def test_every_source_row_lands_and_only_the_overlap_day_repeats(exported):
    landing, summary = exported
    n_days = len(factory.TRADING_DAYS)
    yf_rows = sum(
        pq.read_table(os.path.join(landing, "yfinance_prices", f"drop_date={d}", "part-00000.parquet")).num_rows
        for d in summary["drop_dates"]
    )
    # bronze has 3 tickers x 8 days + 1 planted duplicate; the last drop re-sends one day (3 rows)
    assert summary["rows_by_source"]["yfinance_prices"] == 3 * n_days + 1
    assert yf_rows == 3 * n_days + 1 + 3


def test_drop_one_holds_history_and_reference_tables(exported):
    landing, summary = exported
    first = summary["drop_dates"][0]
    for source in ("constituents", "openfigi_mapping"):
        assert os.path.isdir(os.path.join(landing, source, f"drop_date={first}"))
        assert not os.path.isdir(os.path.join(landing, source, f"drop_date={summary['drop_dates'][1]}"))


def test_landing_files_follow_the_contract(exported):
    landing, summary = exported
    t = pq.read_table(
        os.path.join(landing, "yfinance_prices", f"drop_date={summary['drop_dates'][0]}", "part-00000.parquet")
    )
    types = {f.name: str(f.type) for f in t.schema}
    assert types["date"] == "date32[day]" and types["volume"] == "int64" and types["adj_close"] == "double"
    figi = pq.read_table(
        os.path.join(landing, "openfigi_mapping", f"drop_date={summary['drop_dates'][0]}", "part-00000.parquet")
    )
    assert set(figi.column("snapshot_date").to_pylist()) == {date.fromisoformat(factory.INGEST_DATE)}


def test_conform_refuses_fractional_volume():
    df = factory.yfinance_frame()
    df["volume"] = df["volume"].astype("float64")  # yfinance volume is float64 whenever a NaN is present
    df.loc[0, "volume"] = 10.5
    with pytest.raises(export.ExportError, match="non-integer"):
        export.conform("yfinance_prices", df)


def test_conform_reports_missing_columns():
    with pytest.raises(export.ExportError, match="missing required"):
        export.conform("fred_series", pd.DataFrame({"date": [], "value": []}))


def test_reconciliation_report_parses_the_real_format():
    real = (
        "Reconciliation report — ingest_date=2026-09-08\n"
        "  Compared: 63575 (ticker, date) pairs present in both sources\n"
        "  Flagged (> 0.50% adj_close discrepancy): 1262 (1.99%)\n"
        "  Present only in yfinance (coverage gap, not a discrepancy): 635830\n"
        "  Present only in Tiingo (coverage gap, not a discrepancy): 15361\n"
    )
    assert export.parse_reconciliation_report(real) == {
        "source_ingest_date": "2026-09-08",
        "compared": 63575,
        "flagged": 1262,
        "yfinance_only": 635830,
        "tiingo_only": 15361,
        "tolerance": 0.005,
    }


def test_reference_must_match_the_export_date(tmp_path):
    bronze = factory.write_mdl_bronze(str(tmp_path / "mdl"))
    report = factory.write_recon_report(str(tmp_path / "recon.txt"), ingest_date="2024-02-01")
    with pytest.raises(export.ExportError, match="Report is for"):
        export.export_snapshot(bronze, factory.INGEST_DATE, str(tmp_path / "landing"), reconciliation_report=report)


def test_plan_drops_needs_enough_days():
    with pytest.raises(export.ExportError):
        export.plan_drops([date(2024, 1, 2), date(2024, 1, 3)], 3)


def test_manifest_passes_on_untouched_drops(exported):
    landing, summary = exported
    reports = manifest.verify_landing(landing)
    assert [r.drop_date for r in reports] == summary["drop_dates"]
    assert all(r.ok for r in reports)
    assert sum(r.rows_checked for r in reports) == sum(summary["rows_by_drop"].values())


def test_manifest_catches_a_rewritten_file(exported):
    landing, summary = exported
    d = summary["drop_dates"][1]
    path = os.path.join(landing, "yfinance_prices", f"drop_date={d}", "part-00000.parquet")
    t = pq.read_table(path)
    pq.write_table(t.slice(0, t.num_rows - 1), path)  # one row short
    r = manifest.verify_drop(landing, d)
    assert not r.ok
    assert any("row count mismatch" in p for p in r.problems)
    assert any("sha256 mismatch" in p for p in r.problems)


def test_manifest_catches_missing_and_unexpected_files(exported):
    landing, summary = exported
    d = summary["drop_dates"][0]
    os.remove(os.path.join(landing, "fred_series", f"drop_date={d}", "part-00000.parquet"))
    stray = os.path.join(landing, "tiingo_prices", f"drop_date={d}", "extra.parquet")
    pq.write_table(pq.read_table(os.path.join(landing, "tiingo_prices", f"drop_date={d}", "part-00000.parquet")), stray)
    problems = manifest.verify_drop(landing, d).problems
    assert any(p.startswith("missing file: fred_series") for p in problems)
    assert any(p.startswith("file not in manifest: tiingo_prices") for p in problems)


def test_manifest_missing_is_a_failure(exported):
    landing, summary = exported
    d = summary["drop_dates"][2]
    os.remove(os.path.join(landing, "_manifests", f"drop_date={d}.json"))
    assert "manifest missing" in manifest.verify_drop(landing, d).problems[0]


def test_reference_file_written(exported):
    landing, _ = exported
    with open(os.path.join(landing, "_reference", "reconciliation_expected.json")) as fh:
        ref = json.load(fh)
    assert ref["compared"] == factory.expected_reconciliation()["compared"]
    assert ref["tolerance"] == 0.005


def test_extra_constituents_snapshot_becomes_its_own_drop(tmp_path):
    mdl = str(tmp_path / "mdl")
    bronze = factory.write_mdl_bronze(mdl)
    factory.write_constituents_partition(mdl, factory.LATER_SNAPSHOT, factory.later_constituents_frame())
    landing = str(tmp_path / "landing")
    summary = export.export_snapshot(bronze, factory.INGEST_DATE, landing, extra_constituents=[factory.LATER_SNAPSHOT])
    assert summary["extra_constituent_drops"] == [factory.LATER_SNAPSHOT]
    assert summary["rows_by_drop"][factory.LATER_SNAPSHOT] == 3
    reports = manifest.verify_landing(landing)
    assert [r.drop_date for r in reports][-1] == factory.LATER_SNAPSHOT and all(r.ok for r in reports)
    only = os.listdir(os.path.join(landing, "constituents"))
    assert f"drop_date={factory.LATER_SNAPSHOT}" in only
    assert not os.path.isdir(os.path.join(landing, "yfinance_prices", f"drop_date={factory.LATER_SNAPSHOT}"))


def test_extra_snapshot_must_follow_the_price_drops_and_exist(tmp_path):
    mdl = str(tmp_path / "mdl")
    bronze = factory.write_mdl_bronze(mdl)
    factory.write_constituents_partition(mdl, "2024-01-10", factory.later_constituents_frame("2024-01-10"))
    with pytest.raises(export.ExportError, match="must be later"):
        export.export_snapshot(bronze, factory.INGEST_DATE, str(tmp_path / "a"), extra_constituents=["2024-01-10"])
    with pytest.raises(export.ExportError, match="No constituents bronze"):
        export.export_snapshot(bronze, factory.INGEST_DATE, str(tmp_path / "b"), extra_constituents=["2024-02-01"])
