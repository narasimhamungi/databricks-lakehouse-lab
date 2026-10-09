"""Running the workflow on a local machine, drop by drop.

Two things differ from Databricks:
- A local SparkSession keeps its table catalog in memory, so all tasks must
  run in one process. On Databricks, Unity Catalog persists tables between
  the job's tasks.
- Nothing "arrives" locally, so drops are copied from an exported landing
  folder (the staging folder) into the working landing folder one at a time,
  which is what uploading them to the volume does on Databricks.
"""

from __future__ import annotations

import os
import shutil

from lakehouse.config import LANDING_SOURCES
from lakehouse.manifest import manifest_drop_dates

WORKFLOW = ("setup", "verify_manifest", "bronze", "silver", "gold", "docs_and_tags", "dq_report")
WORK_DIRS = ("landing", "state", "warehouse")


class LocalRunError(RuntimeError):
    pass


def prepare_root(root: str, fresh: bool = False) -> dict[str, str]:
    """Working folders under ``root``. Refuses to reuse a previous run's
    folders unless ``fresh``: the local catalog starts empty every process, so
    old table files would collide with new tables of the same name."""
    paths = {name: os.path.join(root, name) for name in WORK_DIRS}
    leftovers = [p for p in paths.values() if os.path.exists(p)]
    if leftovers and not fresh:
        raise LocalRunError(f"{root} already holds a previous run ({', '.join(leftovers)}); pass --fresh to clear it")
    for p in leftovers:
        shutil.rmtree(p)
    for p in paths.values():
        os.makedirs(p)
    return paths


def drops_in(staging_root: str) -> list[str]:
    dates = manifest_drop_dates(staging_root)
    if not dates:
        raise LocalRunError(f"No manifests under {staging_root}/_manifests; run scripts/export_snapshot.py first")
    return dates


def stage_drop(staging_root: str, landing_root: str, drop_date: str) -> int:
    """Copy one drop (every source, its manifest, and the parity reference if
    there is one) into the working landing folder. Returns the source folders
    copied."""
    copied = 0
    for source in LANDING_SOURCES:
        src = os.path.join(staging_root, source, f"drop_date={drop_date}")
        if os.path.isdir(src):
            shutil.copytree(src, os.path.join(landing_root, source, f"drop_date={drop_date}"), dirs_exist_ok=True)
            copied += 1
    manifest = os.path.join(staging_root, "_manifests", f"drop_date={drop_date}.json")
    if not os.path.isfile(manifest):
        raise LocalRunError(f"No manifest for drop {drop_date} in {staging_root}")
    os.makedirs(os.path.join(landing_root, "_manifests"), exist_ok=True)
    shutil.copy(manifest, os.path.join(landing_root, "_manifests"))
    reference = os.path.join(staging_root, "_reference")
    if os.path.isdir(reference):
        shutil.copytree(reference, os.path.join(landing_root, "_reference"), dirs_exist_ok=True)
    return copied


def run_workflow(spark, ctx, tasks: tuple[str, ...] = WORKFLOW) -> dict:
    from lakehouse.tasks import TASKS

    return {name: TASKS[name](spark, ctx) for name in tasks}
