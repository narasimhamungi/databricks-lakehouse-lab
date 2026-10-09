"""Export a verified marketdata-lakehouse bronze snapshot as landing drops.

Run on the machine that holds marketdata-lakehouse's data folder, from this
repo, after `pip install -e .`:

    python scripts/export_snapshot.py \
        --bronze-root "N:/.../marketdata-lakehouse/data/bronze" \
        --ingest-date 2026-09-08 \
        --landing-root ./landing \
        --reconciliation-report "N:/.../marketdata-lakehouse/outputs/reconciliation_report_2026-09-08.txt" \
        --extra-constituents 2026-10-07

It reads marketdata-lakehouse's files and never writes to that repo. Upload
./landing to the Unity Catalog volume drop by drop (see docs/RUNBOOK.md).
"""

from __future__ import annotations

import argparse
import json
import sys

from lakehouse.export import export_snapshot


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bronze-root", required=True, help="marketdata-lakehouse data/bronze directory")
    p.add_argument("--ingest-date", required=True, help="bronze ingest_date partition to export (YYYY-MM-DD)")
    p.add_argument("--landing-root", default="./landing")
    p.add_argument("--drops", type=int, default=3)
    p.add_argument("--no-overlap", action="store_true", help="do not re-send the previous day in the last drop")
    p.add_argument(
        "--reconciliation-report",
        default=None,
        help="marketdata-lakehouse reconciliation report for the same ingest_date (parity target)",
    )
    p.add_argument(
        "--extra-constituents",
        nargs="*",
        default=[],
        metavar="YYYY-MM-DD",
        help="later bronze ingest dates whose constituents snapshot becomes an extra drop (SCD-2 on a real change)",
    )
    args = p.parse_args(argv)
    summary = export_snapshot(
        args.bronze_root,
        args.ingest_date,
        args.landing_root,
        args.drops,
        overlap=not args.no_overlap,
        reconciliation_report=args.reconciliation_report,
        extra_constituents=args.extra_constituents,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
