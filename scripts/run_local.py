"""Run the whole workflow locally on exported landing drops, one drop at a
time, in a single Spark session; then run it once more with nothing new.

This reproduces what the Databricks job does over the same drops, without a
workspace. Each drop gets its own run (run_id "drop-<date>"), followed by an
idle re-run (run_id "rerun") that should write nothing. The run reports land
in <root>/state/reports, ready for scripts/render_results.py.

Inside Docker (see docs/RUNBOOK.md, "Local reproduction"):

    python /repo/scripts/run_local.py --staging /repo/landing --root /work --fresh

Exit code 1 if any run fails a quality gate or the parity check.
"""

from __future__ import annotations

import argparse
import os
import sys

from lakehouse import local
from lakehouse.config import OHLC_TOLERANCE, LakehouseConfig
from lakehouse.tasks import DataQualityError, RunContext


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--staging", required=True, help="exported landing folder (scripts/export_snapshot.py output)")
    p.add_argument("--root", required=True, help="working folder for landing, state and the table warehouse")
    p.add_argument("--fresh", action="store_true", help="clear a previous run's folders under --root")
    p.add_argument("--warn-tolerance", type=float, default=OHLC_TOLERANCE)
    p.add_argument(
        "--constituents-range",
        nargs=2,
        type=int,
        default=[480, 520],
        metavar=("MIN", "MAX"),
        help="rows a constituents snapshot must have (the original's [480, 520] S&P 500 band)",
    )
    p.add_argument("--no-rerun", action="store_true", help="skip the idle re-run at the end")
    a = p.parse_args(argv)

    paths = local.prepare_root(a.root, fresh=a.fresh)
    drops = local.drops_in(a.staging)
    cfg = LakehouseConfig(
        catalog=None,
        schema_prefix="lakehouse_",
        landing_root=paths["landing"],
        state_root=paths["state"],
        ingest_mode="filestream",
    )

    from lakehouse.spark import get_spark

    spark = get_spark("lakehouse-local", conf={"spark.sql.warehouse.dir": paths["warehouse"]})
    spark.sparkContext.setLogLevel("ERROR")

    runs = [(f"drop-{d}", d) for d in drops] + ([] if a.no_rerun else [("rerun", None)])
    for run_id, drop in runs:
        if drop:
            local.stage_drop(a.staging, cfg.landing_root, drop)
        print(f"\n===== {run_id} =====", flush=True)
        ctx = RunContext(
            cfg=cfg,
            run_id=run_id,
            run_date=drop,
            warn_tolerance=a.warn_tolerance,
            constituents_count_range=tuple(a.constituents_range),
        )
        try:
            local.run_workflow(spark, ctx)
        except DataQualityError as exc:
            print(f"\n{run_id} FAILED: {exc}", file=sys.stderr)
            return 1
    print(f"\nReports: {os.path.join(cfg.state_root, 'reports')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
