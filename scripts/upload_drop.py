"""Upload one landing drop (all sources, its manifest, and the parity
reference) to the Unity Catalog volume with the Databricks CLI.

    python scripts/upload_drop.py --drop-date 2026-09-03 --landing-root ./landing \
        --volume dbfs:/Volumes/workspace/lakehouse_ops/landing

Uploading one drop at a time, then running the job, is what makes the loads
incremental: the job only ever sees what has "arrived".
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

SOURCES = ("yfinance_prices", "tiingo_prices", "constituents", "openfigi_mapping", "fred_series")


def cp(src: str, dst: str, recursive: bool, dry_run: bool) -> None:
    cmd = ["databricks", "fs", "cp", src, dst, "--overwrite"] + (["-r"] if recursive else [])
    print(" ".join(cmd))
    if not dry_run:
        subprocess.run(cmd, check=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--drop-date", required=True)
    p.add_argument("--landing-root", default="./landing")
    p.add_argument("--volume", default="dbfs:/Volumes/workspace/lakehouse_ops/landing")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)

    sent = 0
    for source in SOURCES:
        local = os.path.join(a.landing_root, source, f"drop_date={a.drop_date}")
        if os.path.isdir(local):
            cp(local, f"{a.volume}/{source}/drop_date={a.drop_date}", True, a.dry_run)
            sent += 1
    manifest = os.path.join(a.landing_root, "_manifests", f"drop_date={a.drop_date}.json")
    if not os.path.isfile(manifest):
        sys.exit(f"no manifest for {a.drop_date} at {manifest}")
    cp(manifest, f"{a.volume}/_manifests/drop_date={a.drop_date}.json", False, a.dry_run)
    reference = os.path.join(a.landing_root, "_reference", "reconciliation_expected.json")
    if os.path.isfile(reference):
        cp(reference, f"{a.volume}/_reference/reconciliation_expected.json", False, a.dry_run)
    print(f"{sent} source folders for drop {a.drop_date}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
