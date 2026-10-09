"""Gold: the star schema from marketdata-lakehouse src/model/schema.sql,
rebuilt as Spark transforms. These functions decide *what* changes; delta_io
applies it with MERGE.

Ported as-is:
- dim_date.is_trading_day is empirical: a date is a trading day if the
  yfinance silver table has any row on it. No hand-kept holiday calendar.
- dim_security is SCD-2 on company_name, gics_sector, gics_sub_industry and
  figi. A ticker that leaves the snapshot has its current row closed.
- fact_price_daily keeps both vendors at (security, date, source) grain.
- fact_price_daily_consensus: symmetric adj_close difference against the
  average of the two sources, 0.5% tolerance, yfinance preferred because it
  has the wider coverage and Tiingo is a sample.

Changed on purpose:
- security_key is xxhash64(ticker, effective_from) rather than a SERIAL. A
  deterministic key means rebuilding the dimension gives the same keys, and
  no sequence state has to survive between runs.
- Facts key to the dim_security version in effect on the price date (see
  attach_keys), not to the current row as the original's loaders do.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.config import RECONCILIATION_TOLERANCE

TRACKED_ATTRIBUTES = ("company_name", "gics_sector", "gics_sub_industry", "figi")
DIM_SECURITY_COLUMNS = (
    "security_key",
    "ticker",
    *TRACKED_ATTRIBUTES,
    "effective_from",
    "effective_to",
    "is_current",
)
PRICE_DECIMAL = "decimal(18,6)"


def date_key(col: Column) -> Column:
    return F.date_format(col, "yyyyMMdd").cast("int")


# --- dim_date -------------------------------------------------------------


def build_dim_date(spark: SparkSession, start: str, end: str, trading_dates: DataFrame) -> DataFrame:
    """Every calendar day in [start, end]. ``trading_dates`` has one column,
    ``date``: the days the market actually traded, taken from silver."""
    if start > end:
        raise ValueError(f"dim_date start {start} is after end {end}")
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    traded = trading_dates.select(F.to_date("date").alias("date")).distinct().withColumn("_traded", F.lit(True))
    d = F.col("date")
    dow = (F.dayofweek(d) + 5) % 7  # Spark: 1=Sunday. Target: 0=Monday .. 6=Sunday
    return days.join(traded, "date", "left").select(
        date_key(d).alias("date_key"),
        d.alias("date"),
        F.year(d).cast("smallint").alias("year"),
        F.quarter(d).cast("smallint").alias("quarter"),
        F.month(d).cast("smallint").alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.dayofmonth(d).cast("smallint").alias("day"),
        dow.cast("smallint").alias("day_of_week"),
        F.date_format(d, "EEEE").alias("day_name"),
        (dow < 5).alias("is_weekday"),
        F.coalesce(F.col("_traded"), F.lit(False)).alias("is_trading_day"),
    )


# --- dim_security (SCD-2) -----------------------------------------------------


def security_key(ticker: Column, effective_from: Column) -> Column:
    return F.xxhash64(ticker, effective_from)


def snapshot_with_figi(constituents: DataFrame, figi: DataFrame, snapshot_date: str) -> DataFrame:
    """One constituents snapshot with FIGI left-joined from the latest FIGI
    snapshot on or before it. A ticker without a FIGI keeps its row with a
    NULL figi instead of vanishing from the dimension."""
    snap = constituents.where(F.col("snapshot_date") == F.lit(snapshot_date).cast("date"))
    figi_asof = figi.where(F.col("snapshot_date") <= F.lit(snapshot_date).cast("date"))
    latest = figi_asof.agg(F.max("snapshot_date").alias("d")).collect()[0]["d"]
    if latest is None:
        figi_cols = figi.sparkSession.createDataFrame([], "ticker string, figi string")
    else:
        figi_cols = figi_asof.where(F.col("snapshot_date") == F.lit(latest)).select("ticker", "figi")
    return (
        snap.select("ticker", "company_name", "gics_sector", "gics_sub_industry")
        .join(figi_cols.dropDuplicates(["ticker"]), "ticker", "left")
        .dropDuplicates(["ticker"])
    )


def scd2_classify(current: DataFrame, snapshot: DataFrame) -> DataFrame:
    """Full outer join of the current dimension rows and a new snapshot, with
    a ``status`` of new, changed, removed or unchanged per ticker. NULLs
    compare equal (``<=>``), so a missing FIGI that stays missing is not a
    change, and NULL -> value is."""
    cur = current.select(
        "ticker", *[F.col(a).alias(f"cur_{a}") for a in TRACKED_ATTRIBUTES], F.lit(True).alias("_in_current")
    )
    snap = snapshot.select("ticker", *TRACKED_ATTRIBUTES, F.lit(True).alias("_in_snapshot"))
    joined = snap.join(cur, "ticker", "full_outer")
    differs = F.lit(False)
    for a in TRACKED_ATTRIBUTES:
        differs = differs | ~F.col(a).eqNullSafe(F.col(f"cur_{a}"))
    status = (
        F.when(F.col("_in_current").isNull(), "new")
        .when(F.col("_in_snapshot").isNull(), "removed")
        .when(differs, "changed")
        .otherwise("unchanged")
    )
    return joined.withColumn("status", status)


def scd2_summary(classified: DataFrame) -> dict[str, int]:
    counts = {r["status"]: int(r["count"]) for r in classified.groupBy("status").count().collect()}
    return {
        "new": counts.get("new", 0),
        "changed": counts.get("changed", 0),
        "closed": counts.get("removed", 0),
        "unchanged": counts.get("unchanged", 0),
    }


def scd2_stage(classified: DataFrame, snapshot_date: str) -> DataFrame:
    """Rows for the SCD-2 MERGE (the staged-updates pattern).

    action='close'  merge_key = ticker, matches the current row and closes it
                    (changed and removed tickers).
    action='insert' merge_key = NULL, never matches, so it is inserted as the
                    new current row (new and changed tickers).
    A changed ticker therefore produces one of each.
    """
    snap_date = F.lit(snapshot_date).cast("date")
    attrs = [F.col(a) for a in TRACKED_ATTRIBUTES]
    closes = classified.where(F.col("status").isin("changed", "removed")).select(
        F.col("ticker").alias("merge_key"), F.lit("close").alias("action"), "ticker", *attrs
    )
    inserts = classified.where(F.col("status").isin("new", "changed")).select(
        F.lit(None).cast("string").alias("merge_key"), F.lit("insert").alias("action"), "ticker", *attrs
    )
    return (
        closes.unionByName(inserts)
        .withColumn("snapshot_date", snap_date)
        .withColumn("security_key", security_key(F.col("ticker"), snap_date))
    )


def snapshots_to_apply(constituent_snapshot_dates: list, dim_high_water) -> list[str]:
    """Snapshots newer than anything already in the dimension, oldest first.
    Re-running the same snapshot, or an older one, changes nothing."""
    dates = sorted({str(d) for d in constituent_snapshot_dates if d is not None})
    if dim_high_water is None:
        return dates
    return [d for d in dates if d > str(dim_high_water)]


# --- facts ---------------------------------------------------------------------


def keying_windows(dim_security: DataFrame) -> DataFrame:
    """The date range each dim_security version covers for keying facts.

    A version covers dates from its effective_from up to the next version's
    effective_from, so the versions of a ticker tile the whole timeline and
    every price for a known ticker gets exactly one key:
    - the first version also covers dates before the first snapshot (price
      history starts in 2019; the first snapshot is much later);
    - the latest version stays open, including after a removal from the index
      (the security still has prices; it just left the index);
    - a ticker removed and later re-added has no gap between the two.
    """
    w = Window.partitionBy("ticker")
    ordered = w.orderBy("effective_from")
    first = F.col("effective_from") == F.min("effective_from").over(w)
    return dim_security.select(
        F.col("ticker").alias("_k_ticker"),
        "security_key",
        F.when(first, F.lit(None).cast("date")).otherwise(F.col("effective_from")).alias("_k_from"),
        F.lead("effective_from").over(ordered).alias("_k_to"),
    )


def attach_keys(df: DataFrame, dim_security: DataFrame, dim_date: DataFrame) -> DataFrame:
    """Left-join security_key and date_key, keeping rows that fail to match so
    the caller can count them before dropping.

    Point-in-time: each row keys to the dim_security version in effect on its
    date. The original pipeline keyed every fact to the current row, which
    re-keys history after an attribute change (duplicate facts under the old
    and new key on the next reload) and orphans a removed ticker's history.
    """
    k = keying_windows(dim_security)
    in_window = (
        (F.col("ticker") == F.col("_k_ticker"))
        & (F.col("_k_from").isNull() | (F.col("date") >= F.col("_k_from")))
        & (F.col("_k_to").isNull() | (F.col("date") < F.col("_k_to")))
    )
    dk = dim_date.select("date", "date_key")
    keyed = df.join(F.broadcast(k), in_window, "left").drop("_k_ticker", "_k_from", "_k_to")
    return keyed.join(dk, "date", "left")


def key_match_counts(keyed: DataFrame) -> dict[str, int]:
    row = keyed.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("security_key").isNull().cast("int")).alias("no_sk"),
        F.sum(F.col("date_key").isNull().cast("int")).alias("no_dk"),
    ).collect()[0]
    return {
        "rows": int(row["rows"]),
        "skipped_no_security_key": int(row["no_sk"] or 0),
        "skipped_no_date_key": int(row["no_dk"] or 0),
    }


def matched(keyed: DataFrame) -> DataFrame:
    return keyed.where(F.col("security_key").isNotNull() & F.col("date_key").isNotNull())


def fact_price_daily_rows(keyed: DataFrame, source: str) -> DataFrame:
    dec = [
        F.col(c).cast(PRICE_DECIMAL).alias(c)
        for c in ("open", "high", "low", "close", "adj_open", "adj_high", "adj_low", "adj_close")
    ]
    return matched(keyed).select(
        "security_key",
        "date_key",
        F.lit(source).alias("source"),
        *dec,
        F.col("volume").cast("long").alias("volume"),
        F.col("adj_volume").cast("long").alias("adj_volume"),
    )


def reconcile(yf: DataFrame, tg: DataFrame, tolerance: float = RECONCILIATION_TOLERANCE) -> DataFrame:
    """One consensus row per (ticker, date) across both sources, as in
    marketdata-lakehouse src/reconcile/price_reconciliation.py.

    pct_diff = |yf - tg| / mean(yf, tg); flagged when strictly above the
    tolerance. A pair present in one source only is 'single_source', a
    coverage gap rather than a disagreement.
    """
    y = yf.select("ticker", "date", F.col("adj_close").alias("yf"), F.lit(True).alias("_in_yf"))
    t = tg.select("ticker", "date", F.col("adj_close").alias("tg"), F.lit(True).alias("_in_tg"))
    j = y.join(t, ["ticker", "date"], "full_outer")
    both = F.col("_in_yf").isNotNull() & F.col("_in_tg").isNotNull()
    pct = F.abs(F.col("yf") - F.col("tg")) / ((F.col("yf") + F.col("tg")) / 2)
    return j.select(
        "ticker",
        "date",
        F.when(F.col("_in_yf").isNotNull(), F.col("yf")).otherwise(F.col("tg")).alias("adj_close"),
        F.when(F.col("_in_yf").isNotNull(), "yfinance").otherwise("tiingo").alias("primary_source"),
        F.when(both, F.array(F.lit("yfinance"), F.lit("tiingo")))
        .when(F.col("_in_yf").isNotNull(), F.array(F.lit("yfinance")))
        .otherwise(F.array(F.lit("tiingo")))
        .alias("sources_available"),
        F.when(both, pct).alias("pct_diff"),
        F.when(both & (pct > tolerance), "disagreed")
        .when(both, "agreed")
        .otherwise("single_source")
        .alias("reconciliation_flag"),
    )


def reconciliation_counts(consensus: DataFrame) -> dict[str, int]:
    """The four headline numbers of the original reconciliation report."""
    row = consensus.agg(
        F.sum((F.size("sources_available") == 2).cast("int")).alias("compared"),
        F.sum((F.col("reconciliation_flag") == "disagreed").cast("int")).alias("flagged"),
        F.sum(((F.size("sources_available") == 1) & (F.col("primary_source") == "yfinance")).cast("int")).alias(
            "yf_only"
        ),
        F.sum(((F.size("sources_available") == 1) & (F.col("primary_source") == "tiingo")).cast("int")).alias(
            "tg_only"
        ),
    ).collect()[0]
    return {
        "compared": int(row["compared"] or 0),
        "flagged": int(row["flagged"] or 0),
        "yfinance_only": int(row["yf_only"] or 0),
        "tiingo_only": int(row["tg_only"] or 0),
    }


def fact_consensus_rows(keyed: DataFrame) -> DataFrame:
    return matched(keyed).select(
        "security_key",
        "date_key",
        F.col("adj_close").cast(PRICE_DECIMAL).alias("adj_close"),
        "primary_source",
        "sources_available",
        F.col("pct_diff").cast("decimal(9,6)").alias("pct_diff"),
        "reconciliation_flag",
    )


def fact_macro_rate_rows(fred: DataFrame, dim_date: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (rows to load, rows with no dim_date match)."""
    j = fred.join(dim_date.select("date", "date_key"), "date", "left")
    rows = j.where(F.col("date_key").isNotNull()).select(
        "date_key", "series_id", F.col("value").cast(PRICE_DECIMAL).alias("value")
    )
    return rows, j.where(F.col("date_key").isNull())
