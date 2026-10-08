# Databricks notebook source
# MAGIC %md
# MAGIC # Day 0 capability probe
# MAGIC
# MAGIC Run this once on the Free Edition workspace before building anything. Each check
# MAGIC records PASS or FAIL with the error. **The README's Platform table lists only what
# MAGIC passes here**, and any feature that fails loses its claim.
# MAGIC
# MAGIC Automated here: catalog and schema creation, volume file round-trip, Delta
# MAGIC write / MERGE / history / time travel, Auto Loader with an available-now trigger and
# MAGIC checkpoint (run twice), `_metadata` file columns, OPTIMIZE / VACUUM, comments, tags,
# MAGIC GRANT, row filter. Checked by hand at the end: declarative pipeline, job retries and
# MAGIC failure email, bundle deploy with a personal access token, lineage tab.

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone

RESULTS = []


def check(name, fn):
    try:
        detail = fn()
        RESULTS.append({"check": name, "result": "PASS", "detail": str(detail)[:300] if detail is not None else ""})
    except Exception as exc:  # noqa: BLE001 - the probe records every failure and moves on
        RESULTS.append(
            {"check": name, "result": "FAIL", "detail": (type(exc).__name__ + ": " + str(exc).splitlines()[0])[:300]}
        )
        traceback.print_exc()
    print(RESULTS[-1]["result"], name, "-", RESULTS[-1]["detail"])


# COMMAND ----------

# 1. Catalog: try our own; fall back to the workspace default if creation is blocked.
CATALOG = "lakehouse_lab"


def create_catalog():
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    return CATALOG


check("create catalog", create_catalog)
if RESULTS[-1]["result"] == "FAIL":
    CATALOG = "workspace"
SCHEMA = f"{CATALOG}.lakehouse_probe"
VOLUME = f"/Volumes/{CATALOG}/lakehouse_probe/probe"
print("using", CATALOG)

# COMMAND ----------

check("create schema", lambda: spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}") and SCHEMA)
check("create volume", lambda: spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.probe") and VOLUME)


def volume_roundtrip():
    import os

    os.makedirs(f"{VOLUME}/landing/drop_date=2024-01-02", exist_ok=True)
    with open(f"{VOLUME}/hello.txt", "w") as fh:
        fh.write("probe")
    with open(f"{VOLUME}/hello.txt") as fh:
        assert fh.read() == "probe"
    return "python open() read/write on the volume works"


check("volume file write and read (Python file API)", volume_roundtrip)

# COMMAND ----------

T = f"{SCHEMA}.delta_probe"


def delta_core():
    spark.sql(f"DROP TABLE IF EXISTS {T}")
    spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string").write.format("delta").saveAsTable(T)
    spark.createDataFrame([(2, "B"), (3, "c")], "id int, v string").createOrReplaceTempView("probe_src")
    spark.sql(
        f"MERGE INTO {T} t USING probe_src s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.v = s.v WHEN NOT MATCHED THEN INSERT *"
    )
    hist = spark.sql(f"DESCRIBE HISTORY {T}").select("version", "operation", "operationMetrics").collect()
    v0 = spark.sql(f"SELECT count(*) AS n FROM {T} VERSION AS OF 0").collect()[0]["n"]
    now = spark.table(T).count()
    assert (v0, now) == (2, 3), (v0, now)
    merge = [h for h in hist if h["operation"] == "MERGE"][0]["operationMetrics"]
    return f"versions={len(hist)} v0={v0} now={now} merge_metrics_inserted={merge.get('numTargetRowsInserted')}"


check("Delta write, MERGE, DESCRIBE HISTORY, VERSION AS OF", delta_core)

# COMMAND ----------

LANDING = f"{VOLUME}/landing"
BRONZE = f"{SCHEMA}.autoloader_probe"


def autoloader_twice():
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
    (
        spark.createDataFrame([(1, 1.0)], "id long, px double")
        .write.mode("overwrite")
        .parquet(f"{LANDING}/drop_date=2024-01-02")
    )
    counts = []
    for _ in range(2):
        q = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{VOLUME}/state/schema")
            .option("cloudFiles.partitionColumns", "")
            .load(LANDING)
            .selectExpr("*", "_metadata.file_path AS _source_file")
            .writeStream.option("checkpointLocation", f"{VOLUME}/state/checkpoint")
            .trigger(availableNow=True)
            .toTable(BRONZE)
        )
        q.awaitTermination()
        counts.append(spark.table(BRONZE).count())
    assert counts == [1, 1], counts
    path = spark.table(BRONZE).select("_source_file").first()[0]
    return f"row counts after run 1 and run 2: {counts}; _source_file={path}"


check("Auto Loader available-now + checkpoint, re-run adds 0 rows", autoloader_twice)


def filestream_twice():
    out = f"{SCHEMA}.filestream_probe"
    spark.sql(f"DROP TABLE IF EXISTS {out}")
    counts = []
    for _ in range(2):
        q = (
            spark.readStream.schema("id long, px double")
            .option("recursiveFileLookup", "true")
            .parquet(LANDING)
            .writeStream.option("checkpointLocation", f"{VOLUME}/state/checkpoint_fs")
            .trigger(availableNow=True)
            .toTable(out)
        )
        q.awaitTermination()
        counts.append(spark.table(out).count())
    assert counts == [1, 1], counts
    return f"{counts}"


check("Structured Streaming file source (fallback if Auto Loader fails)", filestream_twice)

# COMMAND ----------


def optimize_vacuum():
    before = spark.sql(f"DESCRIBE DETAIL {T}").first()["numFiles"]
    spark.sql(f"OPTIMIZE {T}")
    after = spark.sql(f"DESCRIBE DETAIL {T}").first()["numFiles"]
    dry = spark.sql(f"VACUUM {T} DRY RUN").count()
    return f"files {before} -> {after}; vacuum dry-run candidates {dry} (default 7-day retention)"


check("OPTIMIZE and VACUUM", optimize_vacuum)
check("table comment", lambda: spark.sql(f"COMMENT ON TABLE {T} IS 'probe'") and "ok")
check("column comment", lambda: spark.sql(f"ALTER TABLE {T} ALTER COLUMN v COMMENT 'probe column'") and "ok")
check("table tags", lambda: spark.sql(f"ALTER TABLE {T} SET TAGS ('layer' = 'probe')") and "ok")


def grant_self():
    me = spark.sql("SELECT current_user()").first()[0]
    spark.sql(f"GRANT SELECT ON TABLE {T} TO `{me}`")
    grants = spark.sql(f"SHOW GRANTS ON TABLE {T}").count()
    return f"granted to {me}; SHOW GRANTS rows={grants}"


check("GRANT / SHOW GRANTS", grant_self)


def row_filter():
    spark.sql(f"CREATE OR REPLACE FUNCTION {SCHEMA}.only_small(id INT) RETURN id < 3")
    spark.sql(f"ALTER TABLE {T} SET ROW FILTER {SCHEMA}.only_small ON (id)")
    n = spark.table(T).count()
    spark.sql(f"ALTER TABLE {T} DROP ROW FILTER")
    return f"rows visible with filter: {n}"


check("row filter (optional - skip the claim if it fails)", row_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual checks
# MAGIC Fill these in after you try them, then re-run the next cell.
# MAGIC - **Declarative pipeline**: `databricks bundle deploy -t dev_pipeline`, then start
# MAGIC   `lakehouse-dq-expectations` in the UI. PASS if the run completes and the expectations tab shows counts.
# MAGIC - **Job with dependencies, retries and failure email**: `databricks bundle run lakehouse_daily`
# MAGIC   with a deliberately broken manifest (edit one byte of a landing file). PASS if the run fails at
# MAGIC   `verify_manifest` and the email arrives.
# MAGIC - **Bundle deploy with a personal access token**: PASS if `databricks bundle deploy` succeeds.
# MAGIC - **Lineage tab**: open a gold table in Catalog Explorer. PASS if upstream silver tables appear.

# COMMAND ----------

MANUAL = {
    "declarative pipeline run": "NOT RUN",
    "job retries + failure email": "NOT RUN",
    "bundle deploy with PAT": "NOT RUN",
    "lineage tab": "NOT RUN",
}
rows = RESULTS + [{"check": k, "result": v, "detail": "manual"} for k, v in MANUAL.items()]
stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
md = ["| Check | Result | Detail |", "|---|---|---|"] + [
    f"| {r['check']} | {r['result']} | {r['detail'].replace('|', '/')} |" for r in rows
]
print(f"Probe run {stamp} on catalog {CATALOG}\n")
print("\n".join(md))
with open(f"{VOLUME}/probe_results.json", "w") as fh:
    json.dump({"run_at": stamp, "catalog": CATALOG, "results": rows}, fh, indent=2)

# COMMAND ----------

# Clean up the probe objects (results file stays in the volume until you drop the schema).
# spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
