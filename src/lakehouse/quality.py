"""Data quality rules, defined once and used two ways.

Each rule is a SQL boolean expression that is TRUE for a good row. The job
path evaluates them with ``F.expr`` and splits rows into clean and quarantine;
the declarative-pipeline path (pipelines/dq_expectations.py) hands the same
strings to expectations. One definition, so the two can't drift.

Severities mirror marketdata-lakehouse's bronze_checks.py:
- "drop": nulls in keys, non-positive prices, negative volume. These mean a
  broken record, so the row is kept out of silver and the gate tolerates none
  by default (zero tolerance in the original).
- "warn": OHLC ordering. A vendor bad print (HUBB 2021-05-05 in ~950K rows)
  is market noise, not a pipeline bug, so the row stays in silver, is copied
  to quarantine, and the gate fails only if the pass share for any one rule
  drops below 99.99% (the original's ``mostly=0.9999``).

Every expression is written null-safe ("x IS NULL OR x > 0") so a NULL means
the same thing to Spark SQL and to pipeline expectations. As in the original,
only the explicit not-null rules fail on NULL.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.config import OHLC_TOLERANCE


@dataclass(frozen=True)
class Rule:
    name: str
    sql: str
    severity: str  # "drop" | "warn"

    def __post_init__(self) -> None:
        if self.severity not in ("drop", "warn"):
            raise ValueError(f"severity must be drop or warn, got {self.severity!r}")


PRICE_RULES: tuple[Rule, ...] = (
    Rule("ticker_not_null", "ticker IS NOT NULL", "drop"),
    Rule("date_not_null", "date IS NOT NULL", "drop"),
    Rule("close_not_null", "close IS NOT NULL", "drop"),
    # Not in the original's bronze checks. Added because the consensus fact
    # declares adj_close NOT NULL, so a row without one cannot be loaded.
    Rule("adj_close_not_null", "adj_close IS NOT NULL", "drop"),
    Rule("close_positive", "close IS NULL OR close > 0", "drop"),
    Rule("open_positive", "open IS NULL OR open > 0", "drop"),
    Rule("adj_close_positive", "adj_close IS NULL OR adj_close > 0", "drop"),
    Rule("volume_non_negative", "volume IS NULL OR volume >= 0", "drop"),
    Rule("high_ge_low", "high IS NULL OR low IS NULL OR high >= low", "warn"),
    Rule("high_ge_open", "high IS NULL OR open IS NULL OR high >= open", "warn"),
    Rule("high_ge_close", "high IS NULL OR close IS NULL OR high >= close", "warn"),
    Rule("low_le_open", "low IS NULL OR open IS NULL OR low <= open", "warn"),
    Rule("low_le_close", "low IS NULL OR close IS NULL OR low <= close", "warn"),
)

CONSTITUENT_RULES: tuple[Rule, ...] = (
    Rule("ticker_not_null", "ticker IS NOT NULL", "drop"),
    Rule("snapshot_date_not_null", "snapshot_date IS NOT NULL", "drop"),
    Rule("gics_sector_not_null", "gics_sector IS NOT NULL", "warn"),
)

FIGI_RULES: tuple[Rule, ...] = (
    Rule("ticker_not_null", "ticker IS NOT NULL", "drop"),
    Rule("snapshot_date_not_null", "snapshot_date IS NOT NULL", "drop"),
)

FRED_RULES: tuple[Rule, ...] = (
    Rule("value_not_null", "value IS NOT NULL", "drop"),
    Rule("date_not_null", "date IS NOT NULL", "drop"),
    # Rates only: CPI is an index level, as in the original's check_fred_bronze.
    Rule(
        "rate_in_range",
        "series_id = 'CPIAUCSL' OR value IS NULL OR (value >= 0 AND value <= 25)",
        "warn",
    ),
    Rule("cpi_positive", "series_id <> 'CPIAUCSL' OR value IS NULL OR value > 0", "drop"),
)

FAILED_RULES_COL = "_failed_rules"


def failed_rules(rules: tuple[Rule, ...]) -> Column:
    """Array of the names of the rules a row breaks (empty when clean)."""
    flags = [F.when(~F.coalesce(F.expr(r.sql), F.lit(False)), F.lit(r.name)) for r in rules]
    return F.array_compact(F.array(*flags))


def drop_rule_names(rules: tuple[Rule, ...]) -> list[str]:
    return [r.name for r in rules if r.severity == "drop"]


def split(df: DataFrame, rules: tuple[Rule, ...]) -> tuple[DataFrame, DataFrame]:
    """Returns (clean, flagged).

    clean    rows that pass every "drop" rule (warn failures stay in).
    flagged  every row that fails any rule, with FAILED_RULES_COL listing them.
    """
    tagged = df.withColumn(FAILED_RULES_COL, failed_rules(rules))
    drop_names = F.array(*[F.lit(n) for n in drop_rule_names(rules)]) if drop_rule_names(rules) else None
    if drop_names is None:
        breaks_drop = F.lit(False)
    else:
        breaks_drop = F.size(F.array_intersect(F.col(FAILED_RULES_COL), drop_names)) > 0
    clean = tagged.where(~breaks_drop).drop(FAILED_RULES_COL)
    flagged = tagged.where(F.size(FAILED_RULES_COL) > 0)
    return clean, flagged


def quarantine_records(flagged: DataFrame, dataset: str, rules: tuple[Rule, ...], run_id: str, run_date) -> DataFrame:
    """Shape flagged rows for the ops.dq_quarantine table: the whole record
    as JSON, so one table can hold rows from datasets with different columns."""
    record_cols = [c for c in flagged.columns if c != FAILED_RULES_COL]
    drop_names = drop_rule_names(rules)
    severity = (
        F.when(F.size(F.array_intersect(F.col(FAILED_RULES_COL), F.array(*[F.lit(n) for n in drop_names]))) > 0, "drop")
        if drop_names
        else F.lit(None)
    )
    return flagged.select(
        F.lit(run_id).alias("run_id"),
        F.lit(run_date).cast("date").alias("run_date"),
        F.lit(dataset).alias("dataset"),
        F.col(FAILED_RULES_COL).alias("failed_rules"),
        F.coalesce(severity, F.lit("warn")).alias("severity"),
        F.to_json(F.struct(*record_cols)).alias("record"),
        (F.col("_drop_date") if "_drop_date" in record_cols else F.lit(None).cast("date")).alias("_drop_date"),
        F.current_timestamp().alias("logged_at"),
    )


def rule_failure_counts(flagged: DataFrame, rules: tuple[Rule, ...]) -> dict[str, int]:
    """Failures per rule name (a row breaking two rules counts once for each)."""
    rows = flagged.select(F.explode(FAILED_RULES_COL).alias("rule")).groupBy("rule").count().collect()
    counts = {r.name: 0 for r in rules}
    counts.update({row["rule"]: int(row["count"]) for row in rows})
    return counts


@dataclass(frozen=True)
class GateResult:
    rule: str
    severity: str
    failures: int
    rows_checked: int
    pass_share: float
    threshold: float
    passed: bool


def evaluate_gate(
    counts: dict[str, int],
    rows_checked: int,
    rules: tuple[Rule, ...],
    max_dropped: int = 0,
    warn_tolerance: float = OHLC_TOLERANCE,
) -> list[GateResult]:
    """Per-rule pass/fail. A drop rule passes while its failures are at most
    ``max_dropped``; a warn rule passes while its pass share is at least
    ``warn_tolerance``. An empty batch passes (there is nothing wrong in it);
    whether an empty batch is itself a problem is the manifest check's job."""
    results = []
    for r in rules:
        fails = counts.get(r.name, 0)
        share = 1.0 if rows_checked == 0 else 1 - fails / rows_checked
        if r.severity == "drop":
            ok, threshold = fails <= max_dropped, float(max_dropped)
        else:
            ok, threshold = share >= warn_tolerance, warn_tolerance
        results.append(GateResult(r.name, r.severity, fails, rows_checked, share, threshold, ok))
    return results
