"""Snapshot export: marketdata-lakehouse bronze -> landing drops for Databricks.

Runs on your machine, against data marketdata-lakehouse has already
ingested and checked. It never calls a market-data API: Free Edition
restricts outbound traffic, and the point is to load verified data.

What it does:
1. Reads one ingest_date partition of each bronze source.
2. Conforms every frame to the landing contract (lakehouse.schemas): fixed
   column names and types, and an error if a value can't be represented
   (fractional volume, for example) rather than a silent cast.
3. Splits prices and FRED by trade date into N dated drops to simulate daily
   arrival: drop 1 holds history, each later drop one more trading day. The
   last drop also re-sends the previous drop's day, the way vendors restate,
   so the MERGE path sees an overlap it must not duplicate.
   Constituents and the FIGI map go in drop 1.
4. Writes a manifest per drop: row count and sha256 per file.
5. Optionally parses the original pipeline's reconciliation report into
   _reference/reconciliation_expected.json, the parity target for dq_report.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse.manifest import build_manifest, write_manifest

MDL_BRONZE_DIRS = {
    "yfinance_prices": "yfinance_prices",
    "tiingo_prices": "tiingo_prices",
    "constituents": "constituents",
    "openfigi_mapping": "openfigi_mapping",
    "fred_series": "fred_series",
}

# Landing contract as pyarrow types; must match lakehouse.schemas.
_D, _F, _I, _S = pa.date32(), pa.float64(), pa.int64(), pa.string()
CONTRACT: dict[str, list[tuple[str, pa.DataType]]] = {
    "yfinance_prices": [
        ("date", _D),
        ("ticker", _S),
        ("open", _F),
        ("high", _F),
        ("low", _F),
        ("close", _F),
        ("adj_close", _F),
        ("volume", _I),
        ("dividends", _F),
        ("stock_splits", _F),
    ],
    "tiingo_prices": [
        ("date", _D),
        ("ticker", _S),
        ("open", _F),
        ("high", _F),
        ("low", _F),
        ("close", _F),
        ("adj_close", _F),
        ("adj_open", _F),
        ("adj_high", _F),
        ("adj_low", _F),
        ("volume", _I),
        ("adj_volume", _F),
    ],
    "constituents": [
        ("ticker", _S),
        ("company_name", _S),
        ("gics_sector", _S),
        ("gics_sub_industry", _S),
        ("date_added", _D),
        ("snapshot_date", _D),
    ],
    "openfigi_mapping": [
        ("ticker", _S),
        ("figi", _S),
        ("composite_figi", _S),
        ("share_class_figi", _S),
        ("name", _S),
        ("security_type", _S),
        ("market_sector", _S),
        ("snapshot_date", _D),
    ],
    "fred_series": [("date", _D), ("series_id", _S), ("value", _F)],
}
REQUIRED = {
    "yfinance_prices": {"date", "ticker", "open", "high", "low", "close", "adj_close", "volume"},
    "tiingo_prices": {"date", "ticker", "open", "high", "low", "close", "adj_close", "volume"},
    "constituents": {"ticker", "company_name", "gics_sector", "gics_sub_industry"},
    "openfigi_mapping": {"ticker", "figi"},
    "fred_series": {"date", "series_id", "value"},
}
DATED_SOURCES = ("yfinance_prices", "tiingo_prices", "fred_series")


class ExportError(ValueError):
    pass


def read_mdl_partition(bronze_root: str, source: str, ingest_date: str) -> pd.DataFrame:
    part = os.path.join(bronze_root, MDL_BRONZE_DIRS[source], f"ingest_date={ingest_date}")
    if not os.path.isdir(part):
        return pd.DataFrame()
    files = sorted(f for f in os.listdir(part) if f.endswith(".parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(os.path.join(part, f)) for f in files], ignore_index=True)


def _to_int64(series: pd.Series, name: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="raise")
    frac = numeric.dropna() % 1
    if (frac != 0).any():
        raise ExportError(f"{name}: {int((frac != 0).sum())} non-integer values; refusing to truncate")
    return numeric.astype("Int64")


def conform(source: str, df: pd.DataFrame, snapshot_date: str | None = None) -> pa.Table:
    """Cast a bronze frame to the landing contract and return an Arrow table."""
    missing = REQUIRED[source] - set(df.columns)
    if missing:
        raise ExportError(f"{source}: missing required columns {sorted(missing)}")
    df = df.copy()
    if "snapshot_date" in dict(CONTRACT[source]) and "snapshot_date" not in df.columns:
        if snapshot_date is None:
            raise ExportError(f"{source}: no snapshot_date column and none supplied")
        df["snapshot_date"] = snapshot_date
    cols = {}
    for name, typ in CONTRACT[source]:
        if name not in df.columns:
            cols[name] = pa.nulls(len(df), type=typ)
            continue
        s = df[name]
        if typ == _D:
            stamps = pd.to_datetime(s, errors="raise")
            if stamps.dt.tz is not None:
                stamps = stamps.dt.tz_localize(None)
            cols[name] = pa.array([None if pd.isna(v) else v.date() for v in stamps], type=typ)
        elif typ == _I:
            cols[name] = pa.array(_to_int64(s, f"{source}.{name}"), type=typ, from_pandas=True)
        elif typ == _F:
            cols[name] = pa.array(pd.to_numeric(s, errors="raise").astype("float64"), type=typ, from_pandas=True)
        else:
            cols[name] = pa.array(s.astype("string"), type=typ, from_pandas=True)
    return pa.table(cols, schema=pa.schema(CONTRACT[source]))


def plan_drops(trading_dates: list[date], n_drops: int = 3) -> list[date]:
    """Drop dates: the last n trading dates. Drop 1 is everything up to the
    first of them, each later drop one more trading day."""
    uniq = sorted(set(trading_dates))
    if n_drops < 1:
        raise ExportError("n_drops must be at least 1")
    if len(uniq) < n_drops:
        raise ExportError(f"Need at least {n_drops} trading dates to make {n_drops} drops, found {len(uniq)}")
    return uniq[-n_drops:]


def split_by_drop(table: pa.Table, drop_dates: list[date], overlap: bool = True) -> list[pa.Table]:
    """Rows for each drop. Drop i holds (drop_dates[i-1], drop_dates[i]]; drop
    1 holds everything up to drop_dates[0]. With ``overlap`` and three or
    more drops, the last drop also re-sends the previous drop's trade date."""
    dates = pd.Series(table.column("date").to_pylist(), dtype="object")
    out = []
    for i, d in enumerate(drop_dates):
        if i == 0:
            mask = dates.map(lambda x, d=d: x is not None and x <= d)
        else:
            lo = drop_dates[i - 1]
            if overlap and i == len(drop_dates) - 1 and i >= 2:
                lo = drop_dates[i - 2]
            mask = dates.map(lambda x, lo=lo, d=d: x is not None and lo < x <= d)
        out.append(table.filter(pa.array(mask.to_numpy(dtype=bool))))
    return out


def write_landing_file(landing_root: str, source: str, drop_date: date, table: pa.Table) -> str:
    out_dir = os.path.join(landing_root, source, f"drop_date={drop_date.isoformat()}")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "part-00000.parquet")
    pq.write_table(table, path)
    return path


REPORT_PATTERNS = {
    "ingest_date": r"ingest_date=(\d{4}-\d{2}-\d{2})",
    "compared": r"Compared:\s*(\d+)",
    "flagged": r"Flagged \(>\s*[\d.]+% adj_close discrepancy\):\s*(\d+)",
    "tolerance_pct": r"Flagged \(>\s*([\d.]+)% adj_close discrepancy\)",
    "yfinance_only": r"Present only in yfinance[^:]*:\s*(\d+)",
    "tiingo_only": r"Present only in Tiingo[^:]*:\s*(\d+)",
}


def parse_reconciliation_report(text: str) -> dict:
    """Headline numbers from marketdata-lakehouse's reconciliation report."""
    out: dict = {}
    for key, pat in REPORT_PATTERNS.items():
        m = re.search(pat, text)
        if not m:
            raise ExportError(f"reconciliation report: could not find {key}")
        out[key] = m.group(1)
    return {
        "source_ingest_date": out["ingest_date"],
        "compared": int(out["compared"]),
        "flagged": int(out["flagged"]),
        "yfinance_only": int(out["yfinance_only"]),
        "tiingo_only": int(out["tiingo_only"]),
        "tolerance": round(float(out["tolerance_pct"]) / 100, 10),
    }


def export_snapshot(
    bronze_root: str,
    ingest_date: str,
    landing_root: str,
    n_drops: int = 3,
    overlap: bool = True,
    reconciliation_report: str | None = None,
) -> dict:
    tables: dict[str, pa.Table] = {}
    for source in MDL_BRONZE_DIRS:
        df = read_mdl_partition(bronze_root, source, ingest_date)
        if df.empty:
            if source == "yfinance_prices":
                raise ExportError(f"No yfinance bronze for ingest_date={ingest_date} under {bronze_root}")
            continue
        tables[source] = conform(source, df, snapshot_date=ingest_date)

    yf_dates = tables["yfinance_prices"].column("date").to_pylist()
    drop_dates = plan_drops([d for d in yf_dates if d is not None], n_drops)

    files_written = 0
    for source, table in tables.items():
        if source in DATED_SOURCES:
            parts = split_by_drop(table, drop_dates, overlap)
        else:
            parts = [table] + [None] * (len(drop_dates) - 1)
        for d, part in zip(drop_dates, parts, strict=True):
            if part is not None and part.num_rows:
                write_landing_file(landing_root, source, d, part)
                files_written += 1

    manifests = [build_manifest(landing_root, d.isoformat()) for d in drop_dates]
    for m in manifests:
        write_manifest(landing_root, m)

    reference = None
    if reconciliation_report:
        with open(reconciliation_report, encoding="utf-8") as fh:
            reference = parse_reconciliation_report(fh.read())
        if reference["source_ingest_date"] != ingest_date:
            raise ExportError(
                f"Report is for ingest_date={reference['source_ingest_date']} but the export is {ingest_date}"
            )
        # The report covers the whole snapshot, so parity only applies once the
        # last drop has been loaded.
        reference["complete_at_drop"] = drop_dates[-1].isoformat()
        os.makedirs(os.path.join(landing_root, "_reference"), exist_ok=True)
        with open(
            os.path.join(landing_root, "_reference", "reconciliation_expected.json"), "w", encoding="utf-8"
        ) as fh:
            json.dump(reference, fh, indent=2)

    return {
        "ingest_date": ingest_date,
        "drop_dates": [d.isoformat() for d in drop_dates],
        "files_written": files_written,
        "rows_by_source": {s: t.num_rows for s, t in tables.items()},
        "rows_by_drop": {m["drop_date"]: m["total_rows"] for m in manifests},
        "reference": reference,
    }
