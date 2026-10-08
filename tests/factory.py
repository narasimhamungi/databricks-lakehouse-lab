"""A small fake marketdata-lakehouse bronze folder, shaped like the real one
(same directory names, same column names and types as each ingest module
writes), with known defects planted so tests can check they are caught."""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd

INGEST_DATE = "2024-01-12"
TRADING_DAYS = [date(2024, 1, 2) + timedelta(days=i) for i in range(10)]
TRADING_DAYS = [d for d in TRADING_DAYS if d.weekday() < 5]  # 8 weekdays, 2024-01-02 .. 01-11

YF_TICKERS = ("AAA", "BBB", "CCC")
TG_TICKERS = ("AAA", "BBB", "DDD")  # DDD is Tiingo-only: a coverage gap
BBB_DISAGREE_DAYS = 2  # first N days BBB's Tiingo adj_close is 1% off


def _bar(base: float, i: int) -> dict:
    close = base + i
    return {"open": close - 0.5, "high": close + 1.0, "low": close - 1.0, "close": close}


def yfinance_frame() -> pd.DataFrame:
    rows = []
    for t_i, t in enumerate(YF_TICKERS):
        for i, d in enumerate(TRADING_DAYS):
            b = _bar(100 + 50 * t_i, i)
            rows.append(
                {
                    "date": pd.Timestamp(d),
                    **b,
                    "adj close": b["close"] * 0.9,
                    "volume": 1000 + i,
                    "dividends": 0.0,
                    "stock splits": 0.0,
                    "ticker": t,
                }
            )
    df = pd.DataFrame(rows).rename(columns={"adj close": "adj_close", "stock splits": "stock_splits"})
    # Planted: one exact duplicate (ticker, date) row, and one OHLC violation.
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    bad = (df["ticker"] == "AAA") & (df["date"] == pd.Timestamp(TRADING_DAYS[2]))
    df.loc[bad, "low"] = df.loc[bad, "open"] + 0.25  # low > open: a vendor bad print
    return df


def tiingo_frame() -> pd.DataFrame:
    rows = []
    for t in TG_TICKERS:
        base = {"AAA": 100, "BBB": 150, "DDD": 400}[t]
        for i, d in enumerate(TRADING_DAYS):
            b = _bar(base, i)
            adj = b["close"] * 0.9
            if t == "BBB" and i < BBB_DISAGREE_DAYS:
                adj *= 1.01
            rows.append(
                {
                    "date": pd.Timestamp(d),
                    **b,
                    "volume": 1000 + i,
                    "adj_close": adj,
                    "adj_open": b["open"] * 0.9,
                    "adj_high": b["high"] * 0.9,
                    "adj_low": b["low"] * 0.9,
                    "adj_volume": float(1000 + i),
                    "ticker": t,
                }
            )
    return pd.DataFrame(rows)


def constituents_frame(snapshot: str = INGEST_DATE, sector_ccc: str = "Utilities") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["AAA", "BBB", "CCC"],
            "company_name": ["Alpha Inc", "Beta Corp", "Gamma plc"],
            "gics_sector": ["Information Technology", "Financials", sector_ccc],
            "gics_sub_industry": ["Software", "Banks", "Electric Utilities"],
            "date_added": pd.to_datetime(["2010-01-01", "2012-06-01", None]),
            "snapshot_date": pd.Timestamp(snapshot),
        }
    )


def figi_frame() -> pd.DataFrame:
    # CCC has no FIGI match, as happens live; it must still reach dim_security.
    return pd.DataFrame(
        {
            "ticker": ["AAA", "BBB"],
            "figi": ["BBG000AAA001", "BBG000BBB002"],
            "composite_figi": ["C1", "C2"],
            "share_class_figi": ["S1", "S2"],
            "name": ["ALPHA", "BETA"],
            "security_type": ["Common Stock"] * 2,
            "market_sector": ["Equity"] * 2,
        }
    )


def fred_frame() -> pd.DataFrame:
    rows = [{"date": pd.Timestamp(d), "series_id": "DGS10", "value": 4.0 + i / 100} for i, d in enumerate(TRADING_DAYS)]
    rows.append({"date": pd.Timestamp("2024-01-01"), "series_id": "CPIAUCSL", "value": 308.4})
    return pd.DataFrame(rows)


def write_mdl_bronze(root: str, ingest_date: str = INGEST_DATE) -> str:
    """Write the fake bronze tree the way marketdata-lakehouse lays it out."""
    bronze = os.path.join(root, "bronze")
    layout = {
        "yfinance_prices": [("batch_0000.parquet", yfinance_frame())],
        "tiingo_prices": [(f"{t}.parquet", g) for t, g in tiingo_frame().groupby("ticker")],
        "constituents": [
            (
                "constituents.parquet",
                constituents_frame().drop(columns=["snapshot_date"]).assign(snapshot_date=pd.Timestamp(ingest_date)),
            )
        ],
        "openfigi_mapping": [("mapping.parquet", figi_frame())],
        "fred_series": [(f"{s}.parquet", g) for s, g in fred_frame().groupby("series_id")],
    }
    for source, files in layout.items():
        part = os.path.join(bronze, source, f"ingest_date={ingest_date}")
        os.makedirs(part, exist_ok=True)
        for name, df in files:
            df.to_parquet(os.path.join(part, name), index=False)
    return bronze


RECON_REPORT = """Reconciliation report — ingest_date={d}
  Compared: {compared} (ticker, date) pairs present in both sources
  Flagged (> 0.50% adj_close discrepancy): {flagged} (12.50%)
  Present only in yfinance (coverage gap, not a discrepancy): {yf_only}
  Present only in Tiingo (coverage gap, not a discrepancy): {tg_only}
"""


def expected_reconciliation() -> dict:
    n = len(TRADING_DAYS)
    return {"compared": 2 * n, "flagged": BBB_DISAGREE_DAYS, "yfinance_only": n, "tiingo_only": n}


def write_recon_report(path: str, ingest_date: str = INGEST_DATE, **override) -> str:
    vals = {**expected_reconciliation(), **override}
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(
            RECON_REPORT.format(
                d=ingest_date,
                compared=vals["compared"],
                flagged=vals["flagged"],
                yf_only=vals["yfinance_only"],
                tg_only=vals["tiingo_only"],
            )
        )
    return path
