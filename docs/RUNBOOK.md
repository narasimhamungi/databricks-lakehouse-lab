# Runbook

From a fresh Databricks Free Edition workspace to the numbers in the README.
Each step says what to keep as evidence. Commands are for PowerShell on Windows;
on macOS or Linux, replace the backtick line breaks with `\`.

## 0. Prerequisites

- A Databricks Free Edition workspace.
- The Databricks CLI with bundle support (`databricks --version`; checked with v1.20.0).
- Python 3.10+ and this repo: `pip install -e ".[dev]"`.
- marketdata-lakehouse's `data/bronze` folder, already ingested and checked.

Authenticate once (OAuth in the browser; no personal access token needed):

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

## 1. Day 0 probe (one hour)

1. Import `notebooks/00_capability_probe.py` (Workspace > Import, or
   `databricks workspace import --format SOURCE --language PYTHON notebooks/00_capability_probe.py /Users/<you>/00_capability_probe`).
2. Open it on serverless compute and Run all.
3. Copy the printed table into the README's Platform section. **Keep only PASS rows as claims.**

Decisions the probe makes for you:

| If this fails | Do this |
|---|---|
| create catalog | Deploy with `--var catalog=workspace` (Free Edition's built-in catalog) and use `workspace` in every path below. |
| Auto Loader | Deploy with `--var ingest_mode=batch`, or `filestream` if that check passed. Don't claim Auto Loader. |
| table tags | `docs_and_tags` reports the failure and carries on. Don't claim tags. |
| row filter | Don't claim masking or row filters. |

## 2. Export the snapshot (on your machine)

The export needs no Spark, so your normal Python works:

```powershell
$MDL = "N:\AAA Projects\Financial projects\Data Eng Quant projects\marketdata-lakehouse"
python -m venv .venv-export
.\.venv-export\Scripts\Activate.ps1
pip install -e . pandas
python scripts\export_snapshot.py --bronze-root "$MDL\data\bronze" --ingest-date 2026-09-08 `
  --landing-root .\landing --extra-constituents 2026-10-07 `
  --reconciliation-report "$MDL\outputs\reconciliation_report_2026-09-08.txt"
python -c "from lakehouse.manifest import verify_landing; [print(r.render()) for r in verify_landing('landing')]"
```

It prints four drops: three price drops (2026-09-04, 2026-09-08, 2026-09-09) and the
2026-10-07 constituents snapshot. Nothing is written to marketdata-lakehouse. If `landing`
already exists from an earlier export, delete it first so no stale drop is left behind.

## 2b. Local reproduction (optional, Docker only)

This runs the same drops through the whole workflow on your machine before you spend
Databricks quota. Run it in PowerShell with Docker Desktop open:

```powershell
docker run --rm -e SPARK_DRIVER_MEMORY=3g `
  -v "N:\AAA Projects\Financial projects\databricks-lakehouse-lab:/repo" -w /work python:3.12-slim-bookworm `
  bash -c "apt-get update -qq && apt-get install -y -qq openjdk-17-jre-headless procps > /dev/null && pip install -q -e '/repo[dev]' && python /repo/scripts/run_local.py --staging /repo/landing --root /work; mkdir -p /repo/outputs/local && rm -rf /repo/outputs/local/reports && cp -r /work/state/reports /repo/outputs/local/"
python scripts\render_results.py outputs\local\reports | Out-File -Encoding utf8 outputs\local\results.md
```

Expect one run per drop, then a re-run that changes no rows, and `parity match` from the
2026-09-09 drop on. Use `python:3.12-slim-bookworm`: the plain `python:3.12-slim` image has
no Java 17.

## 3. Deploy and set up

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run lakehouse_setup -t dev
databricks schemas list lakehouse_lab
```

`lakehouse_setup` creates the `lakehouse_lab` catalog if it is missing, the `lakehouse_bronze`,
`lakehouse_silver`, `lakehouse_gold` and `lakehouse_ops` schemas, and the `landing` and `state`
volumes. The last command must list the four schemas.

## 4. Load drop by drop

Upload one drop, run the job for it, then the next. The loop stops at the first failure; a final
run with no new drop and no `run_date` must change no rows (the idempotency evidence):

```powershell
$V = "dbfs:/Volumes/lakehouse_lab/lakehouse_ops/landing"
$ok = $true
foreach ($D in "2026-09-04","2026-09-08","2026-09-09","2026-10-07") {
  python scripts\upload_drop.py --drop-date $D --landing-root .\landing --volume $V
  if ($LASTEXITCODE -ne 0) { $ok = $false; break }
  databricks bundle run lakehouse_daily -t dev --params run_date=$D
  if ($LASTEXITCODE -ne 0) { $ok = $false; break }
}
if ($ok) { databricks bundle run lakehouse_daily -t dev }
```

The 2026-10-07 drop carries only the later constituents snapshot: expect 6 new and 6 closed
`dim_security` rows. Evidence: a screenshot of the run graph of the final run
(`docs/screenshots/run_graph.png`).

## 5. One deliberate failure

Break one row count in a copy of a manifest, run that drop, then put the original back. The data
files are never touched, so nothing bad can be ingested:

```powershell
$M = "dbfs:/Volumes/lakehouse_lab/lakehouse_ops/landing/_manifests/drop_date=2026-09-04.json"
(Get-Content ".\landing\_manifests\drop_date=2026-09-04.json" -Raw).Replace('"rows": 503,', '"rows": 502,') | Set-Content .\m_broken.json -Encoding ascii -NoNewline
databricks fs cp .\m_broken.json $M --overwrite
databricks bundle run lakehouse_daily -t dev --params run_date=2026-09-04
databricks fs cp ".\landing\_manifests\drop_date=2026-09-04.json" $M --overwrite
Remove-Item .\m_broken.json
```

Expected: `verify_manifest` fails once, every later task is skipped, and the failure email
arrives. "Once" needs `disable_auto_optimization: true` on the task: serverless auto-optimization
otherwise retries failed tasks whatever `max_retries` says (the first run of this step made two
attempts). Evidence: `docs/screenshots/failed_run.png` and `failure_email.png`.

## 6. Delta evidence

```powershell
databricks bundle run lakehouse_delta_evidence -t dev
```

It writes `state/reports/delta_evidence.json`: row counts by version (time travel), schema
enforcement (an extra column without `mergeSchema`, plus how strings written to a DOUBLE column
are treated), schema evolution, bronze's commit history including automatic compactions,
OPTIMIZE on a scratch table with auto-compaction off, and VACUUM.

## 7. Governance

- `docs_and_tags` has already applied the comments and tags. Its log lists anything that failed.
- Open `lakehouse_lab.lakehouse_gold.fact_price_daily_consensus` in Catalog Explorer, then
  Lineage > See lineage graph. Evidence: `docs/screenshots/lineage.png`.

## 8. Optional: declarative pipeline

The `dev_pipeline` target holds only the pipeline (the same quality rules, as expectations) and
`lakehouse_dq_evidence`, which reads the pipeline's event log with
`event_log(TABLE(<one of its materialized views>))` and writes the expectation counts to
`state/reports/pipeline_expectations.json`:

```powershell
databricks bundle deploy -t dev_pipeline --auto-approve
databricks bundle run lakehouse_dq_expectations -t dev_pipeline
databricks bundle run lakehouse_dq_evidence -t dev_pipeline
```

Keep the target deployed. Destroying it deletes the pipeline but leaves its materialized views
behind, still owned by the deleted pipeline, and any later pipeline fails with
"Table ... is already managed by pipeline <id>". Free Edition allows one active pipeline per type;
a triggered pipeline that is not running does not count.

## 9. Collect results for the README

```powershell
databricks fs cp -r dbfs:/Volumes/lakehouse_lab/lakehouse_ops/state/reports .\outputs\reports --overwrite
python scripts\render_results.py .\outputs\reports | Out-File -Encoding utf8 .\outputs\results.md
```

`outputs/reports` is gitignored (raw CLI output can carry account details). Paste `results.md`
into the README's Results section and the probe table into Platform.
`outputs/databricks/` holds the reports behind the README, with the account e-mail replaced by
`workspace-owner`, plus `run_attempts.json` (attempts per run, from `databricks jobs list-runs --expand-tasks`).

## Teardown

`databricks bundle destroy -t dev` and `databricks bundle destroy -t dev_pipeline` remove the jobs
and the pipeline. Then drop the pipeline's leftover views (or everything at once with
`DROP CATALOG lakehouse_lab CASCADE`):

```sql
DROP MATERIALIZED VIEW IF EXISTS lakehouse_lab.lakehouse_ops.dq_yfinance_expectations;
DROP MATERIALIZED VIEW IF EXISTS lakehouse_lab.lakehouse_ops.dq_tiingo_expectations;
```
