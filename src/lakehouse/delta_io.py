"""Writing to Delta. Everything goes through SQL MERGE strings and temp views
rather than the DeltaTable Python API, because serverless Databricks runs on
Spark Connect and SQL is the one surface that behaves the same there, on
classic clusters and in open-source Spark.

The SQL builders are pure functions so they can be unit-tested without the
Delta jars; the execute functions need a Delta-enabled session.
"""

from __future__ import annotations

import json
import uuid

from pyspark.sql import DataFrame, SparkSession


def _q(name: str) -> str:
    return f"`{name}`"


def build_merge_sql(
    target: str,
    source_view: str,
    keys: tuple[str, ...],
    columns: tuple[str, ...],
    compare: tuple[str, ...] | None = None,
    guard: str | None = None,
) -> str:
    """MERGE that inserts new keys and updates a matched row only when a
    compared column actually changed (NULL-safe), so a re-run with the same
    data rewrites nothing. ``guard`` adds a condition on matched rows, e.g.
    only let a newer drop overwrite an older one."""
    compare = compare if compare is not None else tuple(c for c in columns if c not in keys)
    on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in keys)
    changed = " OR ".join(f"NOT (t.{_q(c)} <=> s.{_q(c)})" for c in compare) or "FALSE"
    matched_cond = f"({changed})" if guard is None else f"({guard}) AND ({changed})"
    updates = ", ".join(f"t.{_q(c)} = s.{_q(c)}" for c in columns if c not in keys)
    cols = ", ".join(_q(c) for c in columns)
    vals = ", ".join(f"s.{_q(c)}" for c in columns)
    return (
        f"MERGE INTO {target} t USING {source_view} s ON {on} "
        f"WHEN MATCHED AND {matched_cond} THEN UPDATE SET {updates} "
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )


def build_scd2_merge_sql(target: str, stage_view: str, attributes: tuple[str, ...]) -> str:
    """Staged-updates SCD-2: 'close' rows match the current row by ticker and
    end it on the snapshot date; 'insert' rows carry a NULL merge_key, never
    match, and become the new current row."""
    attr_cols = ", ".join(_q(a) for a in attributes)
    attr_vals = ", ".join(f"s.{_q(a)}" for a in attributes)
    return (
        f"MERGE INTO {target} t USING {stage_view} s "
        f"ON t.ticker = s.merge_key AND t.is_current = true "
        f"WHEN MATCHED AND s.action = 'close' THEN UPDATE SET t.effective_to = s.snapshot_date, t.is_current = false "
        f"WHEN NOT MATCHED AND s.action = 'insert' THEN INSERT "
        f"(security_key, ticker, {attr_cols}, effective_from, effective_to, is_current) "
        f"VALUES (s.security_key, s.ticker, {attr_vals}, s.snapshot_date, CAST(NULL AS DATE), true)"
    )


def ensure_schema(spark: SparkSession, schema: str, comment: str | None = None) -> None:
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    if comment:
        sql += " COMMENT " + json.dumps(comment)
    spark.sql(sql)


def ensure_table(spark: SparkSession, name: str, like: DataFrame) -> bool:
    """Create an empty Delta table with ``like``'s schema. Returns True if it
    was created."""
    if spark.catalog.tableExists(name):
        return False
    like.limit(0).write.format("delta").saveAsTable(name)
    return True


def table_version(spark: SparkSession, name: str) -> int | None:
    if not spark.catalog.tableExists(name):
        return None
    return int(spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").collect()[0]["version"])


def last_operation(spark: SparkSession, name: str) -> dict:
    row = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").collect()[0]
    metrics = row["operationMetrics"] or {}
    return {"version": int(row["version"]), "operation": row["operation"], "metrics": dict(metrics)}


def _temp_view(df: DataFrame, prefix: str) -> str:
    name = f"{prefix}_{uuid.uuid4().hex[:12]}"
    df.createOrReplaceTempView(name)
    return name


def _merge_metrics(spark: SparkSession, target: str, version_before: int | None) -> dict:
    after = last_operation(spark, target)
    if version_before is not None and after["version"] == version_before:
        # Nothing committed: the MERGE had no work to do.
        return {"version": after["version"], "inserted": 0, "updated": 0, "deleted": 0}
    m = after["metrics"]
    return {
        "version": after["version"],
        "inserted": int(m.get("numTargetRowsInserted", 0)),
        "updated": int(m.get("numTargetRowsUpdated", 0)),
        "deleted": int(m.get("numTargetRowsDeleted", 0)),
    }


def merge_upsert(
    spark: SparkSession,
    target: str,
    source: DataFrame,
    keys: tuple[str, ...],
    compare: tuple[str, ...] | None = None,
    guard: str | None = None,
) -> dict:
    ensure_table(spark, target, source)
    before = table_version(spark, target)
    view = _temp_view(source, "src")
    try:
        spark.sql(build_merge_sql(target, view, keys, tuple(source.columns), compare, guard))
    finally:
        spark.catalog.dropTempView(view)
    return {"table": target, **_merge_metrics(spark, target, before)}


def apply_scd2(
    spark: SparkSession, target: str, stage: DataFrame, attributes: tuple[str, ...], empty_dim: DataFrame
) -> dict:
    ensure_table(spark, target, empty_dim)
    before = table_version(spark, target)
    view = _temp_view(stage, "scd2")
    try:
        spark.sql(build_scd2_merge_sql(target, view, attributes))
    finally:
        spark.catalog.dropTempView(view)
    return {"table": target, **_merge_metrics(spark, target, before)}


def append(spark: SparkSession, target: str, df: DataFrame) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)


def detail(spark: SparkSession, name: str) -> dict:
    row = spark.sql(f"DESCRIBE DETAIL {name}").collect()[0]
    return {"num_files": int(row["numFiles"]), "size_bytes": int(row["sizeInBytes"])}
