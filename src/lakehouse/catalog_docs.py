"""Table and column descriptions, and Unity Catalog tags, applied by the
docs_and_tags task. Kept as data so the README's data dictionary and the
catalog say the same thing."""

from __future__ import annotations

TABLE_COMMENTS: dict[tuple[str, str], str] = {
    ("bronze", "yfinance_prices"): "Raw yfinance daily OHLCV as landed, append-only, with source file and drop date.",
    ("bronze", "tiingo_prices"): "Raw Tiingo daily OHLCV (sector-stratified sample) as landed, append-only.",
    ("bronze", "constituents"): "Raw S&P 500 constituents snapshots as landed.",
    ("bronze", "openfigi_mapping"): "Raw OpenFIGI ticker-to-FIGI mapping snapshots as landed.",
    ("bronze", "fred_series"): "Raw FRED macro series (DGS3MO, DGS10, FEDFUNDS, CPIAUCSL) as landed.",
    (
        "silver",
        "prices_yfinance",
    ): "Typed, deduplicated yfinance OHLCV; adjusted O/H/L derived from the adj_close/close factor.",
    ("silver", "prices_tiingo"): "Typed, deduplicated Tiingo OHLCV with Tiingo's own adjusted O/H/L.",
    ("silver", "constituents"): "Constituents, one row per (ticker, snapshot_date).",
    ("silver", "figi_map"): "FIGI map, one row per (ticker, snapshot_date).",
    ("silver", "macro_rate"): "FRED observations, one row per (series_id, date).",
    ("gold", "dim_date"): "Calendar; is_trading_day is true when yfinance silver has any price on the date.",
    ("gold", "dim_security"): "Security dimension, SCD-2 on company_name, GICS sector and sub-industry, and FIGI.",
    ("gold", "fact_price_daily"): "Daily prices at (security, date, source) grain; both vendors kept.",
    (
        "gold",
        "fact_price_daily_consensus",
    ): "One trusted adj_close per (security, date) with the reconciliation outcome as a column.",
    ("gold", "fact_macro_rate"): "FRED macro and rate observations keyed to dim_date.",
    ("ops", "dq_quarantine"): "Rows that broke a quality rule; drop-severity rows are kept out of silver.",
    ("ops", "dq_results"): "Per-run, per-rule quality gate results.",
    ("ops", "task_metrics"): "Rows read, inserted and updated by each task in each run.",
    ("ops", "reconciliation_runs"): "Headline cross-source reconciliation counts per run.",
}

COLUMN_COMMENTS: dict[tuple[str, str], dict[str, str]] = {
    ("gold", "dim_security"): {
        "security_key": "xxhash64(ticker, effective_from): deterministic surrogate key.",
        "effective_from": "Snapshot date this version took effect.",
        "effective_to": "Snapshot date this version was superseded; NULL while current.",
        "is_current": "True for the one in-effect row per ticker.",
    },
    ("gold", "fact_price_daily_consensus"): {
        "primary_source": "Source of adj_close: yfinance when present (full universe), else tiingo.",
        "sources_available": "Which sources had a row for this security and date.",
        "pct_diff": "abs(yfinance - tiingo) / mean of the two; NULL when only one source.",
        "reconciliation_flag": "agreed, disagreed (above 0.5%), or single_source.",
    },
    ("gold", "dim_date"): {
        "date_key": "YYYYMMDD integer.",
        "day_of_week": "0 = Monday .. 6 = Sunday.",
        "is_trading_day": "Derived from the data, not a holiday calendar.",
    },
}

TABLE_TAGS: dict[str, dict[str, str]] = {
    "bronze": {"layer": "bronze", "domain": "market_data", "contains_pii": "false"},
    "silver": {"layer": "silver", "domain": "market_data", "contains_pii": "false"},
    "gold": {"layer": "gold", "domain": "market_data", "contains_pii": "false"},
    "ops": {"layer": "ops", "domain": "pipeline_ops", "contains_pii": "false"},
}
