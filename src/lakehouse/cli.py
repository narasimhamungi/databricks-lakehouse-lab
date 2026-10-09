"""Command-line entry for the workflow tasks.

On Databricks each task is a spark_python_task (jobs/<task>.py) that calls
main(<task>), and Unity Catalog keeps the tables between tasks.

Locally, use scripts/run_local.py instead: a local SparkSession keeps its
table catalog in memory, so tasks run as separate processes would not see
each other's tables.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

from lakehouse.config import LakehouseConfig, databricks_config, on_databricks


def parse_args(task: str, argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog=task, description=f"databricks-lakehouse-lab task: {task}")
    p.add_argument("--catalog", default=None, help="Unity Catalog catalog (Databricks)")
    p.add_argument("--schema-prefix", default="lakehouse_")
    p.add_argument("--run-date", default="", help="YYYY-MM-DD drop to process; empty = all")
    p.add_argument("--run-id", default="", help="job run id; a random one is used when empty")
    p.add_argument("--ingest-mode", default="", choices=["", "autoloader", "filestream", "batch"])
    p.add_argument("--local-root", default="", help="local mode: directory holding landing/ and state/")
    p.add_argument("--max-dropped", type=int, default=0, help="drop-severity failures tolerated per rule")
    p.add_argument(
        "--warn-tolerance",
        type=float,
        default=0.9999,
        help="minimum pass share for warn-severity rules (OHLC ordering)",
    )
    p.add_argument("--parity", default="strict", choices=["strict", "report", "off"])
    p.add_argument("--src-root", default="", help="path added to sys.path before import (jobs bootstrap)")
    # Databricks passes job parameters that render to empty strings when unset;
    # unknown extras are ignored rather than failing the run.
    args, _unknown = p.parse_known_args(argv)
    return args


def build_config(args: argparse.Namespace) -> LakehouseConfig:
    if args.local_root:
        root = os.path.abspath(args.local_root)
        return LakehouseConfig(
            catalog=None,
            schema_prefix=args.schema_prefix,
            landing_root=os.path.join(root, "landing"),
            state_root=os.path.join(root, "state"),
            ingest_mode=args.ingest_mode or "filestream",
        )
    if not args.catalog:
        raise SystemExit("--catalog is required unless --local-root is given")
    return databricks_config(args.catalog, args.schema_prefix, args.ingest_mode or "autoloader")


def main(task: str, argv: list[str] | None = None) -> dict:
    from lakehouse.spark import get_spark
    from lakehouse.tasks import TASKS, RunContext

    args = parse_args(task, argv if argv is not None else sys.argv[1:])
    cfg = build_config(args)
    ctx = RunContext(
        cfg=cfg,
        run_id=args.run_id or uuid.uuid4().hex[:12],
        run_date=args.run_date or None,
        max_dropped=args.max_dropped,
        warn_tolerance=args.warn_tolerance,
        parity=args.parity,
    )
    spark = None if task == "verify_manifest" else get_spark(f"lakehouse-{task}")
    print(
        f"task={task} run_id={ctx.run_id} run_date={ctx.run_date} catalog={cfg.catalog} "
        f"mode={cfg.ingest_mode} databricks={on_databricks()}",
        flush=True,
    )
    return TASKS[task](spark, ctx)
