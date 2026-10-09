"""The two helper scripts: README results rendering and drop upload."""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import render_results  # noqa: E402
import upload_drop  # noqa: E402


def _report(run_id, run_date, parity, recon=None, ref=None, tasks=None):
    tasks = tasks or [
        {"task": "bronze", "table_name": "w.l_bronze.yfinance_prices", "inserted": 19, "updated": 0},
        {"task": "silver", "table_name": "w.l_silver.prices_yfinance", "inserted": 699405, "updated": 0},
        {"task": "silver", "table_name": "w.l_silver.prices_tiingo", "inserted": 18, "updated": 0},
        {
            "task": "gold",
            "table_name": "w.l_gold.dim_security",
            "inserted": 503,
            "updated": 0,
            "note": '{"new": 503, "changed": 0, "closed": 0, "unchanged": 0}',
        },
    ]
    p = {"status": parity}
    if ref:
        p["reference"] = ref
    return {
        "run_id": run_id,
        "run_date": run_date,
        "tasks": tasks,
        "gates_failed": [],
        "quarantine": {"prices_yfinance/warn": 1},
        "reconciliation": recon,
        "parity": p,
    }


def test_render_results_uses_only_report_numbers(tmp_path):
    recon = {"compared": 63575, "flagged": 1262, "yfinance_only": 635830, "tiingo_only": 15361}
    idle = [{"task": "silver", "table_name": "x", "inserted": 0, "updated": 0}] * 3
    reports = [
        _report("drop-2026-09-03", "2026-09-03", "pending"),
        _report("drop-2026-09-08", "2026-09-08", "match", recon, dict(recon)),
        _report("rerun", None, "match", recon, dict(recon), tasks=idle),
    ]
    for r in reports:
        (tmp_path / f"dq_report_{r['run_id']}.json").write_text(json.dumps(r))
    md = render_results.render(str(tmp_path))
    assert "| 2026-09-03 | 19 | 699,405 / 0 | 18 / 0 | 503 / 0 / 0 | 1 | 0 | pending |" in md
    assert "| compared | 63,575 | 63,575 |" in md
    assert "Parity: **match**." in md
    assert md.count("| 2026-") == 2  # the re-run has no drop date, so it is not a table row
    assert "Run `rerun` with no new drop: 0 rows inserted and 0 updated across 3 table writes" in md


def test_upload_drop_dry_run_lists_every_source_and_the_manifest(tmp_path, capsys):
    land = tmp_path / "landing"
    for src in ("yfinance_prices", "fred_series"):
        (land / src / "drop_date=2026-09-03").mkdir(parents=True)
    (land / "_manifests").mkdir()
    (land / "_manifests" / "drop_date=2026-09-03.json").write_text("{}")
    upload_drop.main(["--drop-date", "2026-09-03", "--landing-root", str(land), "--dry-run"])
    out = capsys.readouterr().out
    assert "yfinance_prices/drop_date=2026-09-03" in out and "fred_series/drop_date=2026-09-03" in out
    assert "_manifests/drop_date=2026-09-03.json" in out and "2 source folders" in out


def test_local_prepare_root_refuses_to_reuse_a_previous_run(tmp_path):
    from lakehouse import local

    paths = local.prepare_root(str(tmp_path))
    assert sorted(os.listdir(tmp_path)) == ["landing", "state", "warehouse"]
    (tmp_path / "state" / "old.txt").write_text("x")
    try:
        local.prepare_root(str(tmp_path))
        raise AssertionError("expected LocalRunError")
    except local.LocalRunError:
        pass
    local.prepare_root(str(tmp_path), fresh=True)
    assert os.listdir(paths["state"]) == []


def test_local_stage_drop_copies_one_drop_with_its_manifest(tmp_path):
    from lakehouse import local

    staging = tmp_path / "staging"
    for d in ("2026-09-04", "2026-09-08"):
        (staging / "yfinance_prices" / f"drop_date={d}").mkdir(parents=True)
        (staging / "yfinance_prices" / f"drop_date={d}" / "part-00000.parquet").write_text("x")
    (staging / "_manifests").mkdir()
    (staging / "_manifests" / "drop_date=2026-09-04.json").write_text("{}")
    (staging / "_manifests" / "drop_date=2026-09-08.json").write_text("{}")
    (staging / "_reference").mkdir()
    (staging / "_reference" / "reconciliation_expected.json").write_text("{}")
    assert local.drops_in(str(staging)) == ["2026-09-04", "2026-09-08"]

    landing = tmp_path / "landing"
    assert local.stage_drop(str(staging), str(landing), "2026-09-04") == 1
    assert os.listdir(landing / "yfinance_prices") == ["drop_date=2026-09-04"]
    assert os.listdir(landing / "_manifests") == ["drop_date=2026-09-04.json"]
    assert (landing / "_reference" / "reconciliation_expected.json").exists()
    try:
        local.stage_drop(str(staging), str(landing), "2026-09-09")
        raise AssertionError("expected LocalRunError")
    except local.LocalRunError:
        pass
