"""The workflow tasks. Each takes a SparkSession and a RunContext, does one
stage, records what it did in the ops tables, and returns a summary dict.

    setup -> verify_manifest -> bronze -> silver -> gold -> docs_and_tags -> dq_report

delta_evidence is a separate, manually run job: it demonstrates Delta
features on the built tables and writes the numbers the README quotes.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import bronze as bronze_mod
from lakehouse import delta_io, gold, manifest, quality, silver
from lakehouse.catalog_docs import COLUMN_COMMENTS, TABLE_COMMENTS, TABLE_TAGS
from lakehouse.config import (
    DIM_DATE_START,
    LAYERS,
    OHLC_TOLERANCE,
    PRICE_SOURCES,
    RECONCILIATION_TOLERANCE,
    LakehouseConfig,
)
from lakehouse.schemas import LANDING_SCHEMAS


class DataQualityError(RuntimeError):
    pass


@dataclass
class RunContext:
    cfg: LakehouseConfig
    run_id: str
    run_date: str | None = None  # YYYY-MM-DD: process only this drop in silver
    max_dropped: int = 0
    warn_tolerance: float = OHLC_TOLERANCE
    constituents_count_range: tuple[int, int] = (480, 520)
    parity: str = "strict"  # strict | report | off
    log: list[str] = field(default_factory=list)

    def say(self, msg: str) -> None:
        self.log.append(msg)
        print(msg, flush=True)


# --- helpers -------------------------------------------------------------------


def _ops_append(spark: SparkSession, ctx: RunContext, name: str, rows: list[dict], schema: str) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows, schema).withColumn("logged_at", F.current_timestamp())
    delta_io.append(spark, ctx.cfg.table("ops", name), df)


TASK_METRICS_SCHEMA = (
    "run_id string, run_date string, task string, table_name string, rows_in long, "
    "inserted long, updated long, deleted long, version long, note string"
)
DQ_RESULTS_SCHEMA = (
    "run_id string, run_date string, dataset string, rule string, severity string, failures long, "
    "rows_checked long, pass_share double, threshold double, passed boolean"
)
RECON_SCHEMA = (
    "run_id string, run_date string, compared long, flagged long, yfinance_only long, tiingo_only long, "
    "tolerance double"
)


def _metric(
    ctx: RunContext,
    task: str,
    table: str,
    rows_in=None,
    inserted=None,
    updated=None,
    deleted=None,
    version=None,
    note=None,
) -> dict:
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.run_date,
        "task": task,
        "table_name": table,
        "rows_in": rows_in,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "version": version,
        "note": note,
    }


def _empty(spark: SparkSession, schema) -> DataFrame:
    return spark.createDataFrame([], schema)


# --- setup -----------------------------------------------------------------------


def ensure_catalog(spark, catalog: str) -> bool:
    """Create the Unity Catalog catalog if it does not exist. Returns True
    when it was created. CREATE CATALOG needs the metastore privilege, so the
    error names the fallback instead of failing obscurely."""
    if spark.sql(f"SHOW CATALOGS LIKE '{catalog}'").count():
        return False
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    except Exception as exc:  # noqa: BLE001 - re-raised with the fix
        raise RuntimeError(
            f"cannot create catalog {catalog!r} ({type(exc).__name__}); "
            "deploy with --var catalog=workspace to use the built-in catalog"
        ) from exc
    return True


def setup(spark: SparkSession, ctx: RunContext) -> dict:
    cfg = ctx.cfg
    created = []
    if cfg.catalog and ensure_catalog(spark, cfg.catalog):
        created.append(cfg.catalog)
    for layer in LAYERS:
        delta_io.ensure_schema(spark, cfg.schema(layer), f"databricks-lakehouse-lab {layer} layer")
        created.append(cfg.schema(layer))
    if cfg.catalog:
        for vol in ("landing", "state"):
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('ops')}.{vol}")
            created.append(f"{cfg.schema('ops')}.{vol}")
    else:
        os.makedirs(cfg.landing_root, exist_ok=True)
        os.makedirs(cfg.state_root, exist_ok=True)
    ctx.say(f"setup: {', '.join(created)}")
    return {"created": created}


# --- verify_manifest ---------------------------------------------------------------


def verify_manifest(spark: SparkSession | None, ctx: RunContext) -> dict:
    reports = manifest.verify_landing(ctx.cfg.landing_root, ctx.run_date)
    for r in reports:
        ctx.say(r.render())
    bad = [r for r in reports if not r.ok]
    if bad:
        raise manifest.ManifestError(f"{len(bad)} drop(s) failed the manifest check: {[r.drop_date for r in bad]}")
    return {"drops": [r.drop_date for r in reports], "rows": sum(r.rows_checked for r in reports)}


# --- bronze -------------------------------------------------------------------------


def bronze(spark: SparkSession, ctx: RunContext) -> dict:
    out, metrics = {}, []
    for source, schema in LANDING_SCHEMAS.items():
        if not os.path.isdir(ctx.cfg.landing(source)):
            ctx.say(f"bronze: no landing directory for {source}, skipped")
            continue
        target = ctx.cfg.table("bronze", source)
        res = bronze_mod.ingest(spark, ctx.cfg, source, schema, target)
        ctx.say(f"bronze {source}: +{res['rows_added']} rows ({res['rows_after']} total)")
        out[source] = res
        metrics.append(
            _metric(
                ctx,
                "bronze",
                target,
                rows_in=res["rows_added"],
                inserted=res["rows_added"],
                updated=0,
                deleted=0,
                note=ctx.cfg.ingest_mode,
            )
        )
    _ops_append(spark, ctx, "task_metrics", metrics, TASK_METRICS_SCHEMA)
    return out


# --- silver -------------------------------------------------------------------------


def _bronze_for_run(spark: SparkSession, ctx: RunContext, source: str) -> DataFrame | None:
    name = ctx.cfg.table("bronze", source)
    if not spark.catalog.tableExists(name):
        return None
    df = spark.table(name)
    if ctx.run_date:
        df = df.where(F.col("_drop_date") == F.lit(ctx.run_date).cast("date"))
    return df


def _quality_step(spark, ctx, df, dataset, rules) -> tuple[DataFrame, list, int]:
    """Split one dataset by its rules; write quarantine rows and gate results."""
    rows_checked = df.count()
    clean, flagged = quality.split(df, rules)
    counts = quality.rule_failure_counts(flagged, rules)
    if sum(counts.values()):
        delta_io.append(
            spark,
            ctx.cfg.table("ops", "dq_quarantine"),
            quality.quarantine_records(flagged, dataset, rules, ctx.run_id, ctx.run_date),
        )
    gates = quality.evaluate_gate(
        counts, rows_checked, rules, max_dropped=ctx.max_dropped, warn_tolerance=ctx.warn_tolerance
    )
    _ops_append(
        spark,
        ctx,
        "dq_results",
        [
            {
                "run_id": ctx.run_id,
                "run_date": ctx.run_date,
                "dataset": dataset,
                "rule": g.rule,
                "severity": g.severity,
                "failures": g.failures,
                "rows_checked": g.rows_checked,
                "pass_share": g.pass_share,
                "threshold": g.threshold,
                "passed": g.passed,
            }
            for g in gates
        ],
        DQ_RESULTS_SCHEMA,
    )
    failed = sum(counts.values())
    ctx.say(
        f"quality {dataset}: {rows_checked} rows, {failed} rule failures, "
        f"{'gate ok' if all(g.passed for g in gates) else 'GATE FAILED'}"
    )
    return clean, gates, rows_checked


def silver_task(spark: SparkSession, ctx: RunContext) -> dict:
    cfg, out, metrics = ctx.cfg, {}, []

    for src in PRICE_SOURCES:
        b = _bronze_for_run(spark, ctx, f"{src}_prices")
        if b is None:
            ctx.say(f"silver: no bronze table for {src}, skipped")
            continue
        typed = silver.type_prices(b)
        rows_in = typed.count()
        deduped = silver.dedupe_latest(typed, silver.PRICE_KEYS)
        clean, _, rows_checked = _quality_step(spark, ctx, deduped, f"prices_{src}", quality.PRICE_RULES)
        silver.assert_positive_close(clean)
        rows = silver.to_silver_prices(silver.adjust_prices(clean))
        res = delta_io.merge_upsert(
            spark,
            cfg.table("silver", f"prices_{src}"),
            rows,
            silver.PRICE_KEYS,
            compare=silver.PRICE_VALUE_COLUMNS,
            guard="s._drop_date >= t._drop_date",
        )
        dupes = rows_in - rows_checked
        ctx.say(f"silver prices_{src}: {rows_in} in, {dupes} duplicates, +{res['inserted']} ~{res['updated']}")
        out[f"prices_{src}"] = {**res, "rows_in": rows_in, "duplicates": dupes}
        metrics.append(
            _metric(
                ctx,
                "silver",
                res["table"],
                rows_in,
                res["inserted"],
                res["updated"],
                res["deleted"],
                res["version"],
                note=f"duplicates={dupes}",
            )
        )

    refs = (
        ("constituents", "constituents", ("ticker", "snapshot_date"), quality.CONSTITUENT_RULES),
        ("openfigi_mapping", "figi_map", ("ticker", "snapshot_date"), quality.FIGI_RULES),
        ("fred_series", "macro_rate", ("series_id", "date"), quality.FRED_RULES),
    )
    for bronze_name, silver_name, keys, rules in refs:
        b = _bronze_for_run(spark, ctx, bronze_name)
        if b is None:
            continue
        if "ticker" in b.columns:
            b = b.withColumn("ticker", F.trim("ticker"))
        rows_in = b.count()
        deduped = silver.silver_reference(b, keys)
        clean, _, _ = _quality_step(spark, ctx, deduped, silver_name, rules)
        if silver_name == "constituents":
            _constituent_count_check(spark, ctx, clean)
        res = delta_io.merge_upsert(
            spark,
            cfg.table("silver", silver_name),
            clean,
            keys,
            guard="s._drop_date >= t._drop_date",
            compare=tuple(c for c in clean.columns if c not in keys and c != "_drop_date"),
        )
        ctx.say(f"silver {silver_name}: {rows_in} in, +{res['inserted']} ~{res['updated']}")
        out[silver_name] = {**res, "rows_in": rows_in}
        metrics.append(
            _metric(
                ctx, "silver", res["table"], rows_in, res["inserted"], res["updated"], res["deleted"], res["version"]
            )
        )

    _ops_append(spark, ctx, "task_metrics", metrics, TASK_METRICS_SCHEMA)
    return out


def _constituent_count_check(spark, ctx, constituents: DataFrame) -> None:
    """Ported from check_constituents_bronze: a snapshot outside [480, 520]
    rows means a broken scrape, not index turnover."""
    lo, hi = ctx.constituents_count_range
    rows = []
    for r in constituents.groupBy("snapshot_date").count().collect():
        n = int(r["count"])
        ok = lo <= n <= hi
        rows.append(
            {
                "run_id": ctx.run_id,
                "run_date": ctx.run_date,
                "dataset": "constituents",
                "rule": f"snapshot_{r['snapshot_date']}_row_count_in_{lo}_{hi}",
                "severity": "drop",
                "failures": 0 if ok else 1,
                "rows_checked": n,
                "pass_share": 1.0 if ok else 0.0,
                "threshold": 1.0,
                "passed": ok,
            }
        )
    _ops_append(spark, ctx, "dq_results", rows, DQ_RESULTS_SCHEMA)


# --- gold ---------------------------------------------------------------------------


def _empty_dim_security(spark: SparkSession) -> DataFrame:
    return _empty(
        spark,
        "security_key bigint, ticker string, company_name string, gics_sector string, "
        "gics_sub_industry string, figi string, effective_from date, effective_to date, "
        "is_current boolean",
    )


def _silver_or_empty(spark, ctx, name, schema) -> DataFrame:
    t = ctx.cfg.table("silver", name)
    return spark.table(t) if spark.catalog.tableExists(t) else _empty(spark, schema)


SILVER_PRICE_SCHEMA = (
    "ticker string, date date, open double, high double, low double, close double, adj_close double, "
    "adj_open double, adj_high double, adj_low double, volume long, adj_volume double, _drop_date date, "
    "_source_file string"
)


def gold_task(spark: SparkSession, ctx: RunContext) -> dict:
    cfg, out, metrics = ctx.cfg, {}, []
    yf = _silver_or_empty(spark, ctx, "prices_yfinance", SILVER_PRICE_SCHEMA)
    tg = _silver_or_empty(spark, ctx, "prices_tiingo", SILVER_PRICE_SCHEMA)
    fred = _silver_or_empty(spark, ctx, "macro_rate", "date date, series_id string, value double, _drop_date date")

    # dim_date. Refuse to build from an empty yfinance silver: every date would
    # be marked a non-trading day (the original guards against the same).
    last = yf.agg(F.max("date")).collect()[0][0]
    if last is None:
        raise ValueError("silver.prices_yfinance is empty; refusing to build dim_date from it")
    candidates = [str(last)]
    for df in (tg, fred):
        m = df.agg(F.max("date")).collect()[0][0]
        if m is not None:
            candidates.append(str(m))
    # Not extended to ctx.run_date: a drop that carries only a constituents
    # snapshot would otherwise add calendar days with no prices, all marked
    # as non-trading days.
    end = max(candidates)
    dim_date_df = gold.build_dim_date(spark, DIM_DATE_START, end, yf.select("date"))
    res = delta_io.merge_upsert(spark, cfg.table("gold", "dim_date"), dim_date_df, ("date_key",))
    out["dim_date"] = res
    metrics.append(
        _metric(
            ctx,
            "gold",
            res["table"],
            None,
            res["inserted"],
            res["updated"],
            0,
            res["version"],
            note=f"{DIM_DATE_START}..{end}",
        )
    )

    # dim_security, one snapshot at a time, oldest first.
    dim_name = cfg.table("gold", "dim_security")
    constituents = _silver_or_empty(
        spark,
        ctx,
        "constituents",
        "ticker string, company_name string, gics_sector string, gics_sub_industry string, "
        "date_added date, snapshot_date date, _drop_date date",
    )
    figi = _silver_or_empty(spark, ctx, "figi_map", "ticker string, figi string, snapshot_date date")
    high_water = None
    if spark.catalog.tableExists(dim_name):
        hw = spark.table(dim_name).agg(F.greatest(F.max("effective_from"), F.max("effective_to"))).collect()[0][0]
        high_water = hw
    snap_dates = [r[0] for r in constituents.select("snapshot_date").distinct().collect()]
    scd2_log = []
    for snap in gold.snapshots_to_apply(snap_dates, high_water):
        current = (
            spark.table(dim_name).where("is_current")
            if spark.catalog.tableExists(dim_name)
            else _empty_dim_security(spark)
        )
        classified = gold.scd2_classify(current, gold.snapshot_with_figi(constituents, figi, snap))
        summary = gold.scd2_summary(classified)
        res = delta_io.apply_scd2(
            spark, dim_name, gold.scd2_stage(classified, snap), gold.TRACKED_ATTRIBUTES, _empty_dim_security(spark)
        )
        ctx.say(f"gold dim_security snapshot {snap}: {summary}")
        scd2_log.append({"snapshot_date": snap, **summary, **res})
        metrics.append(
            _metric(
                ctx,
                "gold",
                dim_name,
                None,
                res["inserted"],
                res["updated"],
                0,
                res["version"],
                note=json.dumps(summary),
            )
        )
    if not spark.catalog.tableExists(dim_name):
        raise ValueError("gold.dim_security does not exist and no constituents snapshot was available to build it")
    out["dim_security"] = scd2_log

    # Every version, not just current rows: each price keys to the version in
    # effect on its date (see gold.attach_keys).
    dim_versions = spark.table(dim_name)
    dim_date_tbl = spark.table(cfg.table("gold", "dim_date"))

    # fact_price_daily: both vendors, (security, date, source) grain.
    parts, key_counts = [], {}
    for src, df in (("yfinance", yf), ("tiingo", tg)):
        keyed = gold.attach_keys(df, dim_versions, dim_date_tbl)
        key_counts[src] = gold.key_match_counts(keyed)
        parts.append(gold.fact_price_daily_rows(keyed, src))
    facts = parts[0].unionByName(parts[1])
    res = delta_io.merge_upsert(
        spark, cfg.table("gold", "fact_price_daily"), facts, ("security_key", "date_key", "source")
    )
    out["fact_price_daily"] = {**res, "key_matching": key_counts}
    metrics.append(
        _metric(
            ctx,
            "gold",
            res["table"],
            None,
            res["inserted"],
            res["updated"],
            0,
            res["version"],
            note=json.dumps(key_counts),
        )
    )

    # Consensus over the full silver tables, so the counts are comparable with
    # the original pipeline's whole-snapshot reconciliation report.
    consensus = gold.reconcile(yf, tg)
    recon = gold.reconciliation_counts(consensus)
    keyed = gold.attach_keys(consensus, dim_versions, dim_date_tbl)
    res = delta_io.merge_upsert(
        spark,
        cfg.table("gold", "fact_price_daily_consensus"),
        gold.fact_consensus_rows(keyed),
        ("security_key", "date_key"),
    )
    out["fact_price_daily_consensus"] = {**res, "reconciliation": recon, "key_matching": gold.key_match_counts(keyed)}
    metrics.append(
        _metric(
            ctx, "gold", res["table"], None, res["inserted"], res["updated"], 0, res["version"], note=json.dumps(recon)
        )
    )
    _ops_append(
        spark,
        ctx,
        "reconciliation_runs",
        [
            {
                "run_id": ctx.run_id,
                "run_date": ctx.run_date,
                "compared": recon["compared"],
                "flagged": recon["flagged"],
                "yfinance_only": recon["yfinance_only"],
                "tiingo_only": recon["tiingo_only"],
                "tolerance": RECONCILIATION_TOLERANCE,
            }
        ],
        RECON_SCHEMA,
    )
    ctx.say(f"gold reconciliation: {recon}")

    rows, unmatched = gold.fact_macro_rate_rows(fred, dim_date_tbl)
    res = delta_io.merge_upsert(spark, cfg.table("gold", "fact_macro_rate"), rows, ("date_key", "series_id"))
    out["fact_macro_rate"] = {**res, "skipped_no_date_key": unmatched.count()}
    metrics.append(_metric(ctx, "gold", res["table"], None, res["inserted"], res["updated"], 0, res["version"]))

    _ops_append(spark, ctx, "task_metrics", metrics, TASK_METRICS_SCHEMA)
    return out


# --- docs and tags ------------------------------------------------------------------


def _sql_str(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def docs_and_tags(spark: SparkSession, ctx: RunContext) -> dict:
    """Table and column comments everywhere; tags only where Unity Catalog
    supports them. Each statement is tried on its own and reported, so one
    unsupported feature doesn't hide the rest."""
    applied, failed = [], []

    def run(sql: str) -> None:
        try:
            spark.sql(sql)
            applied.append(sql)
        except Exception as exc:  # noqa: BLE001 - report and continue, by design
            failed.append({"sql": sql, "error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:200]})

    for (layer, name), comment in TABLE_COMMENTS.items():
        table = ctx.cfg.table(layer, name)
        if not spark.catalog.tableExists(table):
            continue
        run(f"COMMENT ON TABLE {table} IS {_sql_str(comment)}")
        for col, ccomment in COLUMN_COMMENTS.get((layer, name), {}).items():
            run(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT {_sql_str(ccomment)}")
        if ctx.cfg.catalog:
            tags = ", ".join(f"{_sql_str(k)} = {_sql_str(v)}" for k, v in TABLE_TAGS[layer].items())
            run(f"ALTER TABLE {table} SET TAGS ({tags})")
    ctx.say(f"docs_and_tags: {len(applied)} statements applied, {len(failed)} failed")
    for f in failed:
        ctx.say(f"  failed: {f['error']}")
    return {"applied": len(applied), "failed": failed}


# --- dq_report ----------------------------------------------------------------------


def _reference(ctx: RunContext) -> dict | None:
    path = os.path.join(ctx.cfg.reference_dir, "reconciliation_expected.json")
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _loaded_through(spark: SparkSession, ctx: RunContext) -> str | None:
    t = ctx.cfg.table("silver", "prices_yfinance")
    if not spark.catalog.tableExists(t):
        return None
    d = spark.table(t).agg(F.max("_drop_date")).collect()[0][0]
    return str(d) if d is not None else None


def parity_check(actual: dict, expected: dict) -> list[str]:
    problems = []
    if abs(float(expected.get("tolerance", RECONCILIATION_TOLERANCE)) - RECONCILIATION_TOLERANCE) > 1e-12:
        problems.append(f"tolerance differs: reference {expected.get('tolerance')}, port {RECONCILIATION_TOLERANCE}")
    for k in ("compared", "flagged", "yfinance_only", "tiingo_only"):
        if k in expected and int(expected[k]) != int(actual.get(k, -1)):
            problems.append(f"{k}: reference {expected[k]}, port {actual.get(k)}")
    return problems


def dq_report(spark: SparkSession, ctx: RunContext) -> dict:
    cfg = ctx.cfg
    results_t = cfg.table("ops", "dq_results")
    gates = (
        spark.table(results_t).where(F.col("run_id") == ctx.run_id).collect()
        if spark.catalog.tableExists(results_t)
        else []
    )
    failed_gates = [g.asDict() for g in gates if not g["passed"]]

    q_t = cfg.table("ops", "dq_quarantine")
    quarantine = {}
    if spark.catalog.tableExists(q_t):
        for r in spark.table(q_t).where(F.col("run_id") == ctx.run_id).groupBy("dataset", "severity").count().collect():
            quarantine[f"{r['dataset']}/{r['severity']}"] = int(r["count"])

    recon_t = cfg.table("ops", "reconciliation_runs")
    recon_rows = (
        spark.table(recon_t).where(F.col("run_id") == ctx.run_id).orderBy(F.desc("logged_at")).limit(1).collect()
        if spark.catalog.tableExists(recon_t)
        else []
    )
    recon = recon_rows[0].asDict() if recon_rows else None

    parity = {"status": "no reference"}
    ref = _reference(ctx)
    if ref and recon and ctx.parity != "off":
        loaded = _loaded_through(spark, ctx)
        complete_at = ref.get("complete_at_drop")
        if complete_at and (loaded is None or loaded < complete_at):
            parity = {
                "status": "pending",
                "detail": f"reference covers the snapshot through drop {complete_at}; silver loaded through {loaded}",
            }
        else:
            problems = parity_check(recon, ref)
            parity = {"status": "match" if not problems else "MISMATCH", "reference": ref, "problems": problems}

    metrics_t = cfg.table("ops", "task_metrics")
    task_rows = []
    if spark.catalog.tableExists(metrics_t):
        cols = ("task", "table_name", "rows_in", "inserted", "updated", "version", "note")
        task_rows = [
            {c: r[c] for c in cols}
            for r in spark.table(metrics_t).where(F.col("run_id") == ctx.run_id).orderBy("logged_at").collect()
        ]

    report = {
        "run_id": ctx.run_id,
        "run_date": ctx.run_date,
        "tasks": task_rows,
        "gates_checked": len(gates),
        "gates_failed": failed_gates,
        "quarantine": quarantine,
        "reconciliation": {k: recon[k] for k in ("compared", "flagged", "yfinance_only", "tiingo_only")}
        if recon
        else None,
        "parity": parity,
    }
    _write_report(ctx, f"dq_report_{ctx.run_id}.json", report)
    ctx.say(json.dumps(report, indent=2, default=str))

    reasons = []
    if failed_gates:
        reasons.append(
            f"{len(failed_gates)} quality gate(s) failed: {[g['dataset'] + '.' + g['rule'] for g in failed_gates]}"
        )
    if parity["status"] == "MISMATCH" and ctx.parity == "strict":
        reasons.append("reconciliation counts do not match the reference: " + "; ".join(parity["problems"]))
    if reasons:
        raise DataQualityError(" | ".join(reasons))
    return report


def _write_report(ctx: RunContext, filename: str, payload: dict) -> str:
    out_dir = os.path.join(ctx.cfg.state_root, "reports")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    return path


# --- delta_evidence -----------------------------------------------------------------


def _timed(spark: SparkSession, sql: str, repeats: int = 3) -> list[float]:
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        times.append(round(time.perf_counter() - t0, 3))
    return times


def _try_append(df: DataFrame, table: str) -> dict:
    try:
        df.write.format("delta").mode("append").saveAsTable(table)
        return {"rejected": False}
    except Exception as exc:  # noqa: BLE001 - AnalysisException locally, Spark Connect types on serverless
        return {"rejected": True, "error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:200]}


def _history_summary(spark: SparkSession, table: str) -> list[dict]:
    """Each commit's operation, whether Databricks ran it automatically
    (auto-compaction), and the files it added and removed."""
    rows = (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .select("version", "timestamp", "userName", "operation", "operationParameters", "operationMetrics")
        .collect()
    )
    out = []
    for h in sorted(rows, key=lambda h: h["version"]):
        params, metrics = h["operationParameters"] or {}, h["operationMetrics"] or {}
        out.append(
            {
                "version": int(h["version"]),
                "timestamp": str(h["timestamp"]),
                "user": h["userName"],
                "operation": h["operation"],
                "auto": params.get("auto"),
                "files_added": metrics.get("numAddedFiles") or metrics.get("numFiles"),
                "files_removed": metrics.get("numRemovedFiles"),
            }
        )
    return out


def _predictive_optimization(spark: SparkSession, schema: str) -> str | None:
    """The schema's predictive-optimization setting as Databricks reports it
    (e.g. 'ENABLE (inherited from METASTORE ...)'); None where not reported."""
    try:
        rows = spark.sql(f"DESCRIBE SCHEMA EXTENDED {schema}").collect()
    except Exception:  # noqa: BLE001 - not available on open-source Spark
        return None
    for r in rows:
        values = list(r.asDict().values())
        if values and "predictive" in str(values[0]).lower():
            return str(values[1]) if len(values) > 1 else None
    return None


def _compaction_demo(spark: SparkSession, table: str, appends: int = 8) -> dict:
    """OPTIMIZE where it has work to do: one file per small append, with
    auto-compaction and optimized writes off for this table only."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT) USING DELTA TBLPROPERTIES "
            "('delta.autoOptimize.autoCompact' = 'false', 'delta.autoOptimize.optimizeWrite' = 'false')"
        )
        for i in range(appends):
            spark.range(i, i + 1).coalesce(1).write.format("delta").mode("append").saveAsTable(table)
        before = delta_io.detail(spark, table)["num_files"]
        spark.sql(f"OPTIMIZE {table}")
        result = {"appends": appends, "files_before": before, "files_after": delta_io.detail(spark, table)["num_files"]}
    except Exception as exc:  # noqa: BLE001 - evidence job records the failure and carries on
        result = {"error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:200]}
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    return result


def delta_evidence(spark: SparkSession, ctx: RunContext) -> dict:
    """Time travel, schema enforcement and evolution, OPTIMIZE and VACUUM,
    on the tables the pipeline built. Writes delta_evidence.json."""
    cfg = ctx.cfg
    ev: dict = {}

    silver_t = cfg.table("silver", "prices_yfinance")
    hist = spark.sql(f"DESCRIBE HISTORY {silver_t}").select("version", "operation", "operationMetrics").collect()
    versions = sorted(int(h["version"]) for h in hist)
    ev["history"] = [
        {
            "version": int(h["version"]),
            "operation": h["operation"],
            "inserted": (h["operationMetrics"] or {}).get("numTargetRowsInserted"),
            "updated": (h["operationMetrics"] or {}).get("numTargetRowsUpdated"),
        }
        for h in sorted(hist, key=lambda h: h["version"])
    ]
    ev["time_travel"] = {
        f"v{v}": spark.sql(f"SELECT count(*) AS n FROM {silver_t} VERSION AS OF {v}").collect()[0]["n"]
        for v in versions
    }

    # Schema enforcement. The canonical Delta case is an append that adds a
    # column without mergeSchema. The two type cases record how the platform
    # treats a string written to a DOUBLE column: on Databricks serverless the
    # malformed value failed as a cast error (CAST_INVALID_INPUT), i.e. the
    # write attempted an implicit cast rather than rejecting the type outright.
    scratch = cfg.table("ops", "evidence_schema_demo")
    spark.sql(f"DROP TABLE IF EXISTS {scratch}")
    spark.createDataFrame([(1, 1.5)], "id int, price double").write.format("delta").saveAsTable(scratch)
    cases = {
        "extra_column_without_mergeSchema": _try_append(
            spark.createDataFrame([(2, 1.0, "x")], "id int, price double, note string"), scratch
        ),
        "malformed_string_into_double": _try_append(
            spark.createDataFrame([(2, "not a number")], "id int, price string"), scratch
        ),
    }
    spark.createDataFrame([(3, 2.5, "added later")], "id int, price double, note string").write.format("delta").mode(
        "append"
    ).option("mergeSchema", "true").saveAsTable(scratch)
    ev["schema_evolution"] = {
        "columns_after": spark.table(scratch).columns,
        "rows_with_null_note": spark.table(scratch).where("note IS NULL").count(),
    }
    spark.sql(f"DROP TABLE IF EXISTS {scratch}")
    spark.createDataFrame([(1, 1.5)], "id int, price double").write.format("delta").saveAsTable(scratch)
    cases["numeric_string_into_double"] = _try_append(
        spark.createDataFrame([(4, "3.25")], "id int, price string"), scratch
    )
    if not cases["numeric_string_into_double"]["rejected"]:
        cases["numeric_string_into_double"]["stored_value"] = spark.table(scratch).where("id = 4").first()["price"]
    spark.sql(f"DROP TABLE IF EXISTS {scratch}")
    canonical = cases["extra_column_without_mergeSchema"]
    ev["schema_enforcement"] = {"rejected": canonical["rejected"], "error": canonical.get("error"), "cases": cases}

    # OPTIMIZE on the real bronze table, plus its write history: Databricks
    # auto-compaction can leave little for a manual OPTIMIZE to do.
    bronze_t = cfg.table("bronze", "yfinance_prices")
    probe_sql = f"SELECT ticker, count(*) AS n, max(close) AS mx FROM {bronze_t} WHERE ticker LIKE 'A%' GROUP BY ticker"
    before = delta_io.detail(spark, bronze_t)
    t_before = _timed(spark, probe_sql)
    spark.sql(f"OPTIMIZE {bronze_t}")
    after = delta_io.detail(spark, bronze_t)
    t_after = _timed(spark, probe_sql)
    ev["optimize"] = {
        "table": bronze_t,
        "files_before": before["num_files"],
        "files_after": after["num_files"],
        "query_seconds_before": t_before,
        "query_seconds_after": t_after,
        "note": "three runs each; first runs include cache warm-up",
        "bronze_history": _history_summary(spark, bronze_t),
        "predictive_optimization": _predictive_optimization(spark, cfg.schema("bronze")),
        "compaction_demo": _compaction_demo(spark, cfg.table("ops", "evidence_compaction_demo")),
    }
    try:
        dry = spark.sql(f"VACUUM {bronze_t} DRY RUN").collect()
        spark.sql(f"VACUUM {bronze_t}")
        ev["vacuum"] = {
            "retention": "default (7 days)",
            "files_eligible": len(dry),
            "note": "files replaced by OPTIMIZE are kept until the retention window passes",
        }
    except Exception as exc:  # noqa: BLE001
        ev["vacuum"] = {"error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:200]}

    path = _write_report(ctx, "delta_evidence.json", ev)
    ctx.say(json.dumps(ev, indent=2, default=str))
    ctx.say(f"delta_evidence written to {path}")
    return ev


# A materialized view the declarative pipeline publishes; event_log(TABLE(..))
# on it returns the whole pipeline's event log without a separate log table.
PIPELINE_ANCHOR_VIEW = "dq_yfinance_prices_checked"


def _as_dict(value) -> dict:
    """event_log() returns origin/details as JSON strings or structs depending
    on the runtime; accept either."""
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value) if value else {}
    if isinstance(value, dict):
        return value
    return value.asDict(recursive=True)


def event_rows(rows: list) -> list[dict]:
    """Normalise event_log() rows to the dicts summarise_expectations reads."""
    out = []
    for r in rows:
        r = r if isinstance(r, dict) else r.asDict(recursive=True)
        origin = _as_dict(r.get("origin"))
        out.append(
            {
                "event_type": r.get("event_type"),
                "update_id": origin.get("update_id"),
                "flow_name": origin.get("flow_name"),
                "timestamp": r.get("timestamp"),
                "details": _as_dict(r.get("details")),
            }
        )
    return out


def summarise_expectations(events: list[dict]) -> dict:
    """Expectation results per dataset for the latest pipeline update, from
    event-log rows with event_type, update_id, flow_name, timestamp, details.

    details is the event's JSON (a string in the published event log). Only
    the flow's COMPLETED events are counted when it has any, so a progress
    event that repeats the totals is not counted twice."""
    updates = [e for e in events if e["event_type"] == "create_update"]
    latest = max(updates, key=lambda e: e["timestamp"])["update_id"] if updates else None
    per_flow: dict[str, list[tuple[str, dict]]] = {}
    for e in events:
        if e["event_type"] != "flow_progress" or (latest and e["update_id"] != latest):
            continue
        details = json.loads(e["details"]) if isinstance(e["details"], str) else (e["details"] or {})
        progress = details.get("flow_progress") or {}
        if progress.get("data_quality"):
            per_flow.setdefault(e["flow_name"], []).append((progress.get("status"), progress["data_quality"]))
    flows = {}
    for flow, items in per_flow.items():
        completed = [dq for status, dq in items if status == "COMPLETED"]
        chosen = completed or [dq for _, dq in items]
        expectations: dict[str, dict] = {}
        dropped = 0
        for dq in chosen:
            dropped += int(dq.get("dropped_records") or 0)
            for x in dq.get("expectations") or []:
                agg = expectations.setdefault(x["name"], {"passed": 0, "failed": 0})
                agg["passed"] += int(x.get("passed_records") or 0)
                agg["failed"] += int(x.get("failed_records") or 0)
        flows[flow] = {"dropped_records": dropped, "expectations": expectations}
    return {"update_id": latest, "flows": flows}


def pipeline_evidence(spark: SparkSession, ctx: RunContext) -> dict:
    """Read the declarative pipeline's event log through one of its
    materialized views and write the expectation counts of the latest update
    to pipeline_expectations.json. Fails if there are none, so a silent pass
    is impossible.

    The pipeline does not publish its log to a named table: a published log
    outlives the pipeline when the dev_pipeline target is destroyed, and the
    next deploy (a new pipeline id) then fails with TABLE_OR_VIEW_ALREADY_EXISTS."""
    view = ctx.cfg.table("ops", PIPELINE_ANCHOR_VIEW)
    rows = spark.sql(f"SELECT event_type, origin, timestamp, details FROM event_log(TABLE({view}))").collect()
    summary = summarise_expectations(event_rows(rows))
    if not summary["flows"]:
        raise RuntimeError(f"no expectation results for the latest update in the event log of {view}")
    path = _write_report(ctx, "pipeline_expectations.json", summary)
    ctx.say(json.dumps(summary, indent=2, default=str))
    ctx.say(f"pipeline expectations written to {path}")
    return summary


TASKS = {
    "setup": setup,
    "verify_manifest": verify_manifest,
    "bronze": bronze,
    "silver": silver_task,
    "gold": gold_task,
    "docs_and_tags": docs_and_tags,
    "dq_report": dq_report,
    "delta_evidence": delta_evidence,
    "pipeline_evidence": pipeline_evidence,
}
