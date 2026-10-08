"""Manifests: the export step records each landing file's row count and
sha256, and the first task of every run checks them before anything is read.

A drop that arrives truncated, re-encoded or with a stray file fails here,
loudly, instead of producing a plausible-looking bronze table. This is plain
Python on purpose: Unity Catalog volumes are readable as ordinary paths on
Databricks compute, and the check should not depend on Spark being healthy.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field

import pyarrow.parquet as pq

from lakehouse.config import LANDING_SOURCES

DROP_DIR = re.compile(r"^drop_date=(\d{4}-\d{2}-\d{2})$")
MANIFEST_FILE = re.compile(r"^drop_date=(\d{4}-\d{2}-\d{2})\.json$")


class ManifestError(RuntimeError):
    pass


def sha256_of(path: str, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def parquet_rows(path: str) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def drop_files(landing_root: str, drop_date: str) -> list[str]:
    """Landing-relative paths of every file in one drop, all sources."""
    found = []
    for source in LANDING_SOURCES:
        drop_dir = os.path.join(landing_root, source, f"drop_date={drop_date}")
        if not os.path.isdir(drop_dir):
            continue
        for name in sorted(os.listdir(drop_dir)):
            full = os.path.join(drop_dir, name)
            if os.path.isfile(full):
                found.append(f"{source}/drop_date={drop_date}/{name}")
    return found


def build_manifest(landing_root: str, drop_date: str) -> dict:
    files = {}
    for rel in drop_files(landing_root, drop_date):
        full = os.path.join(landing_root, rel)
        files[rel] = {"rows": parquet_rows(full), "sha256": sha256_of(full), "bytes": os.path.getsize(full)}
    if not files:
        raise ManifestError(f"No landing files for drop_date={drop_date} under {landing_root}")
    return {"drop_date": drop_date, "files": files, "total_rows": sum(f["rows"] for f in files.values())}


def write_manifest(landing_root: str, manifest: dict) -> str:
    out_dir = os.path.join(landing_root, "_manifests")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"drop_date={manifest['drop_date']}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    return path


def manifest_drop_dates(landing_root: str) -> list[str]:
    mdir = os.path.join(landing_root, "_manifests")
    if not os.path.isdir(mdir):
        return []
    dates = [m.group(1) for m in (MANIFEST_FILE.match(n) for n in os.listdir(mdir)) if m]
    return sorted(dates)


def landed_drop_dates(landing_root: str) -> list[str]:
    dates: set[str] = set()
    for source in LANDING_SOURCES:
        src_dir = os.path.join(landing_root, source)
        if not os.path.isdir(src_dir):
            continue
        dates.update(m.group(1) for m in (DROP_DIR.match(n) for n in os.listdir(src_dir)) if m)
    return sorted(dates)


@dataclass
class ManifestReport:
    drop_date: str
    files_checked: int = 0
    rows_checked: int = 0
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems

    def render(self) -> str:
        status = "PASS" if self.ok else "FAIL"
        head = f"{status} drop_date={self.drop_date}: {self.files_checked} files, {self.rows_checked} rows"
        return "\n".join([head, *(f"  - {p}" for p in self.problems)])


def verify_drop(landing_root: str, drop_date: str) -> ManifestReport:
    report = ManifestReport(drop_date=drop_date)
    mpath = os.path.join(landing_root, "_manifests", f"drop_date={drop_date}.json")
    if not os.path.isfile(mpath):
        report.problems.append(f"manifest missing: {mpath}")
        return report
    with open(mpath, encoding="utf-8") as fh:
        expected = json.load(fh)["files"]

    present = set(drop_files(landing_root, drop_date))
    for rel in sorted(set(expected) - present):
        report.problems.append(f"missing file: {rel}")
    for rel in sorted(present - set(expected)):
        report.problems.append(f"file not in manifest: {rel}")

    for rel in sorted(set(expected) & present):
        full = os.path.join(landing_root, rel)
        want = expected[rel]
        rows = parquet_rows(full)
        digest = sha256_of(full)
        report.files_checked += 1
        report.rows_checked += rows
        if rows != want["rows"]:
            report.problems.append(f"row count mismatch: {rel} has {rows}, manifest says {want['rows']}")
        if digest != want["sha256"]:
            report.problems.append(f"sha256 mismatch: {rel}")
    return report


def verify_landing(landing_root: str, drop_date: str | None = None) -> list[ManifestReport]:
    """Check one drop, or every drop that has files or a manifest."""
    if drop_date:
        dates = [drop_date]
    else:
        dates = sorted(set(landed_drop_dates(landing_root)) | set(manifest_drop_dates(landing_root)))
    if not dates:
        raise ManifestError(f"Nothing to verify under {landing_root}")
    return [verify_drop(landing_root, d) for d in dates]
