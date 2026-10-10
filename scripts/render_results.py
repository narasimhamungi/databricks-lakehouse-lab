"""Turn the run reports the pipeline wrote into the README's Results tables.

Download the reports from the volume first:

    databricks fs cp -r dbfs:/Volumes/workspace/lakehouse_ops/state/reports ./outputs/reports

then:

    python scripts/render_results.py ./outputs/reports > outputs/results.md

Every number in the output comes from a report file. Nothing is typed by hand,
so the README cannot drift from what actually ran.
"""

from __future__ import annotations

import glob
import json
import os
import sys


def _load(paths: list[str]) -> list[dict]:
    out = []
    for p in sorted(paths):
        with open(p, encoding="utf-8-sig") as fh:  # tolerate a BOM from PowerShell
            out.append(json.load(fh))
    return out


def _row(task_rows: list[dict], task: str, table_suffix: str) -> dict | None:
    for r in task_rows:
        if r["task"] == task and str(r["table_name"]).endswith(table_suffix):
            return r
    return None


def _n(v) -> str:
    return "-" if v is None else f"{v:,}"


def _iu(r: dict | None) -> str:
    return f"{_n(r['inserted'])} / {_n(r['updated'])}" if r else "-"


def _scd2(task_rows: list[dict]) -> str:
    """new / changed / closed summed over the snapshots a run applied."""
    totals, seen = {"new": 0, "changed": 0, "closed": 0}, False
    for r in task_rows:
        if r["task"] == "gold" and str(r["table_name"]).endswith(".dim_security") and r.get("note"):
            note = json.loads(r["note"])
            seen = True
            for k in totals:
                totals[k] += int(note.get(k, 0))
    return f"{totals['new']} / {totals['changed']} / {totals['closed']}" if seen else "-"


def render(report_dir: str) -> str:
    reports = _load(glob.glob(os.path.join(report_dir, "dq_report_*.json")))
    runs = sorted((r for r in reports if r.get("run_date")), key=lambda r: r["run_date"])
    reruns = [r for r in reports if not r.get("run_date")]
    lines = ["### Drop-by-drop loads", ""]
    lines.append(
        "| Drop | Bronze rows added (yfinance) | Silver yfinance inserted / updated | "
        "Silver Tiingo inserted / updated | dim_security new / changed / closed | Quarantined rows | "
        "Gates failed | Parity |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in runs:
        t = r.get("tasks", [])
        b = _row(t, "bronze", ".yfinance_prices")
        sy = _row(t, "silver", ".prices_yfinance")
        st = _row(t, "silver", ".prices_tiingo")
        q = sum(r.get("quarantine", {}).values())
        lines.append(
            f"| {r['run_date']} | {_n(b['inserted']) if b else '-'} | {_iu(sy)} | {_iu(st)} | {_scd2(t)} | {q} | "
            f"{len(r['gates_failed'])} | {r['parity']['status']} |"
        )

    for r in reruns:
        t = r.get("tasks", [])
        ins = sum(int(x.get("inserted") or 0) for x in t)
        upd = sum(int(x.get("updated") or 0) for x in t)
        lines.append(
            f"\nRun `{r['run_id']}` with no new drop: {_n(ins)} rows inserted and {_n(upd)} updated across "
            f"{len(t)} table writes; {len(r['gates_failed'])} gates failed; parity {r['parity']['status']}."
        )

    final = runs[-1] if runs else None
    if final and final.get("reconciliation"):
        rec = final["reconciliation"]
        lines += [
            "",
            "### Reconciliation (full snapshot)",
            "",
            "| | Port (Spark) | Original (pandas) |",
            "|---|---|---|",
        ]
        ref = final["parity"].get("reference", {})
        for k in ("compared", "flagged", "yfinance_only", "tiingo_only"):
            original = f"{ref[k]:,}" if k in ref else "-"
            lines.append(f"| {k} | {rec[k]:,} | {original} |")
        lines.append(f"\nParity: **{final['parity']['status']}**.")

    ev_path = os.path.join(report_dir, "delta_evidence.json")
    if os.path.isfile(ev_path):
        ev = _load([ev_path])[0]
        opt = ev["optimize"]
        lines += [
            "",
            "### Delta Lake evidence",
            "",
            f"- Time travel: row count by version of silver yfinance {ev['time_travel']}.",
            *_enforcement_lines(ev["schema_enforcement"]),
            f"- Schema evolution: columns after mergeSchema append {ev['schema_evolution']['columns_after']}.",
            f"- OPTIMIZE on {opt['table']}: {opt['files_before']} files -> {opt['files_after']}. "
            f"Query seconds before {opt['query_seconds_before']}, after {opt['query_seconds_after']} "
            f"({opt['note']}).",
            *_compaction_lines(opt),
            f"- VACUUM: {ev.get('vacuum')}.",
        ]
    pe_path = os.path.join(report_dir, "pipeline_expectations.json")
    if os.path.isfile(pe_path):
        lines += _pipeline_lines(_load([pe_path])[0])
    return "\n".join(lines) + "\n"


def _pipeline_lines(pe: dict) -> list[str]:
    """Expectation counts from the declarative pipeline's event log."""
    out = [
        "",
        "### Declarative pipeline expectations",
        "",
        "| Dataset | Expectation | Passed | Failed |",
        "|---|---|---|---|",
    ]
    dropped = []
    for flow, f in sorted(pe["flows"].items()):
        name = flow.split(".")[-1]
        for exp, c in sorted(f["expectations"].items()):
            out.append(f"| {name} | {exp} | {c['passed']:,} | {c['failed']:,} |")
        dropped.append(f"{name} {f['dropped_records']:,}")
    out.append(f"\nRows dropped by expect_or_drop: {', '.join(dropped)}.")
    return out


def _enforcement_lines(se: dict) -> list[str]:
    cases = se.get("cases")
    if not cases:  # reports from before the cases were split out
        return [f"- Schema enforcement: mismatched append rejected = {se['rejected']}."]
    out = ["- Schema enforcement:"]
    for name, c in cases.items():
        verdict = "rejected" if c["rejected"] else f"accepted (stored {c.get('stored_value', '?')})"
        out.append(f"  - {name.replace('_', ' ')}: {verdict}.")
    return out


def _compaction_lines(opt: dict) -> list[str]:
    out = []
    hist = opt.get("bronze_history") or []
    auto = [h for h in hist if h["operation"] == "OPTIMIZE" and str(h.get("auto")).lower() == "true"]
    if hist:
        out.append(f"- Bronze history: {len(hist)} commits, {len(auto)} of them automatic compactions (auto=true).")
        for h in hist:
            if h["operation"] == "OPTIMIZE":
                who = f" at {h['timestamp']} by {h['user']}" if h.get("timestamp") else ""
                out.append(
                    f"  - OPTIMIZE v{h.get('version', '?')}{who}: removed {h.get('files_removed')} files, "
                    f"added {h.get('files_added')}."
                )
    if opt.get("predictive_optimization"):
        out.append(f"- Predictive optimization on the bronze schema: {opt['predictive_optimization']}.")
    demo = opt.get("compaction_demo") or {}
    if "files_before" in demo:
        out.append(
            f"- OPTIMIZE with auto-compaction off ({demo['appends']} single-row appends): "
            f"{demo['files_before']} files -> {demo['files_after']}."
        )
    elif "error" in demo:
        out.append(f"- Compaction demo failed: {demo['error']}.")
    return out


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: render_results.py <reports-dir>")
    sys.stdout.write(render(sys.argv[1]))
