"""End-to-end on Delta: the whole workflow over three landing drops, then a
re-run, then a constituents change. Needs the Delta jars (CI has them).

The pipeline is built once per module. Tests run in file order: read-only
checks first, then tests that add runs or change data, SCD-2 last.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace

import pandas as pd
import pytest
from pyspark.sql import functions as F

from lakehouse import export, local, manifest, tasks
from lakehouse.config import LakehouseConfig
from lakehouse.tasks import RunContext
from tests import factory

pytestmark = pytest.mark.delta
BUILD_RUNS = ("run0", "run1", "run2")


def _stage(tmp_path):
    staging = str(tmp_path / "staging")
    report = factory.write_recon_report(str(tmp_path / "recon.txt"))
    summary = export.export_snapshot(
        factory.write_mdl_bronze(str(tmp_path / "mdl")), factory.INGEST_DATE, staging, reconciliation_report=report
    )
    return staging, summary


def _run_all(spark, ctx):
    return local.run_workflow(spark, ctx)


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("e2e")
    cfg = LakehouseConfig(
        catalog=None,
        schema_prefix="e2e_",
        landing_root=str(tmp_path / "landing"),
        state_root=str(tmp_path / "state"),
        ingest_mode="filestream",
    )
    staging, summary = _stage(tmp_path)
    runs = []
    for run_id, d in zip(BUILD_RUNS, summary["drop_dates"], strict=True):
        local.stage_drop(staging, cfg.landing_root, d)
        # One planted OHLC violation in 24 rows: relax the warn tolerance so the
        # happy path passes; test_gate_fails_at_default_tolerance covers the rest.
        ctx = RunContext(cfg=cfg, run_id=run_id, run_date=d, warn_tolerance=0.9, constituents_count_range=(1, 1000))
        runs.append(_run_all(spark, ctx))
    return cfg, summary, runs


def test_bronze_appends_each_drop_once(spark, built):
    cfg, summary, runs = built
    n = len(factory.TRADING_DAYS)
    added = [r["bronze"]["yfinance_prices"]["rows_added"] for r in runs]
    assert added == [3 * (n - 2) + 1, 3, 6]  # drop 3 re-sends drop 2's day
    assert spark.table(cfg.table("bronze", "yfinance_prices")).count() == sum(added)


def test_silver_is_deduplicated_and_the_overlap_day_is_not_rewritten(spark, built):
    cfg, _, runs = built
    n = len(factory.TRADING_DAYS)
    assert spark.table(cfg.table("silver", "prices_yfinance")).count() == 3 * n
    assert spark.table(cfg.table("silver", "prices_tiingo")).count() == 3 * n
    last = runs[-1]["silver"]["prices_yfinance"]
    assert (last["inserted"], last["updated"]) == (3, 0)


def test_quarantine_holds_the_planted_bad_print(spark, built):
    cfg, _, _ = built
    q = spark.table(cfg.table("ops", "dq_quarantine")).where(F.col("run_id").isin(*BUILD_RUNS)).collect()
    assert len(q) == 1 and q[0]["severity"] == "warn" and q[0]["failed_rules"] == ["low_le_open"]
    assert json.loads(q[0]["record"])["ticker"] == "AAA"


def test_gold_dimensions_and_facts(spark, built):
    cfg, _, _ = built
    n = len(factory.TRADING_DAYS)
    dim = {r["ticker"]: r for r in spark.table(cfg.table("gold", "dim_security")).collect()}
    assert set(dim) == {"AAA", "BBB", "CCC"} and all(r["is_current"] for r in dim.values())
    assert dim["CCC"]["figi"] is None and dim["AAA"]["figi"] == "BBG000AAA001"
    facts = spark.table(cfg.table("gold", "fact_price_daily"))
    # DDD is Tiingo-only and not a constituent: counted as skipped, not loaded.
    assert facts.count() == 3 * n + 2 * n
    trading = spark.table(cfg.table("gold", "dim_date")).where("is_trading_day").count()
    assert trading == n


def test_reconciliation_matches_the_reference_report(spark, built):
    cfg, _, runs = built
    recon = runs[-1]["gold"]["fact_price_daily_consensus"]["reconciliation"]
    assert recon == factory.expected_reconciliation()
    # The reference covers the full snapshot: earlier drops report "pending".
    assert [r["dq_report"]["parity"]["status"] for r in runs] == ["pending", "pending", "match"]


def test_rerunning_everything_changes_nothing(spark, built):
    cfg, summary, _ = built
    ctx = RunContext(cfg=cfg, run_id="rerun", run_date=None, warn_tolerance=0.9, constituents_count_range=(1, 1000))
    out = _run_all(spark, ctx)
    assert all(v["rows_added"] == 0 for v in out["bronze"].values())
    for name in ("prices_yfinance", "prices_tiingo", "constituents", "macro_rate"):
        assert (out["silver"][name]["inserted"], out["silver"][name]["updated"]) == (0, 0), name
    for name in ("dim_date", "fact_price_daily", "fact_price_daily_consensus", "fact_macro_rate"):
        assert (out["gold"][name]["inserted"], out["gold"][name]["updated"]) == (0, 0), name
    assert out["gold"]["dim_security"] == []  # no newer snapshot to apply


def test_gate_fails_at_default_tolerance(spark, built):
    cfg, _, _ = built
    ctx = RunContext(cfg=cfg, run_id="strict", constituents_count_range=(1, 1000))
    tasks.silver_task(spark, ctx)  # re-checks every row; the planted OHLC row is 1 in 24
    with pytest.raises(tasks.DataQualityError, match="low_le_open"):
        tasks.dq_report(spark, ctx)


def test_parity_mismatch_fails_the_report(spark, built):
    cfg, _, _ = built
    path = os.path.join(cfg.reference_dir, "reconciliation_expected.json")
    with open(path) as fh:
        ref = json.load(fh)
    original = dict(ref)
    ref["flagged"] += 1
    with open(path, "w") as fh:
        json.dump(ref, fh)
    try:
        ctx = RunContext(cfg=cfg, run_id="parity", warn_tolerance=0.9, constituents_count_range=(1, 1000))
        tasks.gold_task(spark, ctx)
        with pytest.raises(tasks.DataQualityError, match="flagged"):
            tasks.dq_report(spark, ctx)
        assert tasks.dq_report(spark, replace(ctx, parity="report"))["parity"]["status"] == "MISMATCH"
    finally:
        with open(path, "w") as fh:
            json.dump(original, fh)


def test_batch_mode_is_also_idempotent(spark, built):
    cfg, _, _ = built
    batch = replace(cfg, ingest_mode="batch", schema_prefix=cfg.schema_prefix + "b_")
    ctx = RunContext(cfg=batch, run_id="b1")
    tasks.setup(spark, ctx)
    first = tasks.bronze(spark, ctx)["yfinance_prices"]["rows_added"]
    second = tasks.bronze(spark, ctx)["yfinance_prices"]["rows_added"]
    assert first > 0 and second == 0


def test_delta_evidence(spark, built):
    cfg, _, _ = built
    ev = tasks.delta_evidence(spark, RunContext(cfg=cfg, run_id="ev"))
    assert len(ev["time_travel"]) >= 3
    assert ev["schema_enforcement"]["rejected"] is True
    cases = ev["schema_enforcement"]["cases"]
    assert cases["extra_column_without_mergeSchema"]["rejected"] is True
    assert cases["malformed_string_into_double"]["rejected"] is True
    assert "numeric_string_into_double" in cases  # platform-dependent: recorded, not asserted
    assert "note" in ev["schema_evolution"]["columns_after"]
    assert ev["schema_evolution"]["rows_with_null_note"] == 1
    assert ev["optimize"]["files_after"] <= ev["optimize"]["files_before"]
    assert [h["version"] for h in ev["optimize"]["bronze_history"]] == sorted(
        h["version"] for h in ev["optimize"]["bronze_history"]
    )
    demo = ev["optimize"]["compaction_demo"]
    if "error" not in demo:  # table properties are Databricks-specific on older OSS Delta
        assert demo["files_after"] < demo["files_before"]
    assert os.path.isfile(os.path.join(cfg.state_root, "reports", "delta_evidence.json"))


def test_scd2_merge_on_a_new_snapshot(spark, built):
    cfg, _, _ = built
    new_snap = factory.LATER_SNAPSHOT
    export.write_landing_file(
        cfg.landing_root,
        "constituents",
        pd.Timestamp(new_snap).date(),
        export.conform("constituents", factory.later_constituents_frame()),
    )
    manifest.write_manifest(cfg.landing_root, manifest.build_manifest(cfg.landing_root, new_snap))
    facts_before = spark.table(cfg.table("gold", "fact_price_daily")).count()
    ctx = RunContext(cfg=cfg, run_id="snap2", run_date=new_snap, warn_tolerance=0.9, constituents_count_range=(1, 1000))
    out = _run_all(spark, ctx)
    assert out["gold"]["dim_security"][0]["new"] == 1
    assert out["gold"]["dim_security"][0]["changed"] == 1
    assert out["gold"]["dim_security"][0]["closed"] == 1

    dim = spark.table(cfg.table("gold", "dim_security"))
    assert dim.count() == 5
    current = {r["ticker"]: r for r in dim.where("is_current").collect()}
    assert set(current) == {"AAA", "CCC", "EEE"} and current["CCC"]["gics_sector"] == "Industrials"
    closed = {(r["ticker"], str(r["effective_to"])) for r in dim.where("NOT is_current").collect()}
    assert closed == {("BBB", new_snap), ("CCC", new_snap)}
    assert dim.groupBy("security_key").count().where(F.col("count") > 1).count() == 0

    # Point-in-time keying: the removed (BBB) and reclassified (CCC) tickers'
    # price history keeps its keys, so the fact reload adds nothing.
    n = len(factory.TRADING_DAYS)
    fact = out["gold"]["fact_price_daily"]
    assert fact["inserted"] == 0 and spark.table(cfg.table("gold", "fact_price_daily")).count() == facts_before
    assert fact["key_matching"]["yfinance"]["skipped_no_security_key"] == 0
    assert fact["key_matching"]["tiingo"]["skipped_no_security_key"] == n  # DDD was never a constituent
    # A constituents-only drop does not stretch the calendar past the prices.
    last_day = spark.table(cfg.table("gold", "dim_date")).agg(F.max("date")).collect()[0][0]
    assert last_day == factory.TRADING_DAYS[-1]
    assert out["dq_report"]["parity"]["status"] == "match"
