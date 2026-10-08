"""The two helper scripts: README results rendering and drop upload."""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import render_results  # noqa: E402
import upload_drop  # noqa: E402


def _report(run_date, parity, recon=None, ref=None):
    tasks = [
        {"task": "bronze", "table_name": "w.l_bronze.yfinance_prices", "inserted": 19, "updated": 0},
        {"task": "silver", "table_name": "w.l_silver.prices_yfinance", "inserted": 18, "updated": 0},
        {"task": "silver", "table_name": "w.l_silver.prices_tiingo", "inserted": 18, "updated": 0},
    ]
    p = {"status": parity}
    if ref:
        p["reference"] = ref
    return {
        "run_id": run_date,
        "run_date": run_date,
        "tasks": tasks,
        "gates_failed": [],
        "quarantine": {"prices_yfinance/warn": 1},
        "reconciliation": recon,
        "parity": p,
    }


def test_render_results_uses_only_report_numbers(tmp_path):
    recon = {"compared": 63575, "flagged": 1262, "yfinance_only": 635830, "tiingo_only": 15361}
    reports = [_report("2026-09-03", "pending"), _report("2026-09-08", "match", recon, dict(recon))]
    for r in reports:
        (tmp_path / f"dq_report_{r['run_id']}.json").write_text(json.dumps(r))
    (tmp_path / "dq_report_adhoc.json").write_text(json.dumps({**reports[0], "run_date": None}))
    md = render_results.render(str(tmp_path))
    assert "| 2026-09-03 | 19 | 18 / 0 | 18 / 0 | 1 | 0 | pending |" in md
    assert "| compared | 63,575 | 63,575 |" in md
    assert "Parity: **match**." in md
    assert md.count("| 2026-") == 2  # the ad-hoc run without a drop date is left out


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
