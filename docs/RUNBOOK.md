# Runbook

From a fresh Databricks Free Edition workspace to the numbers in the README.
Each step says what to keep as evidence. Commands are for PowerShell on Windows;
on macOS or Linux, replace the backtick line breaks with `\`.

## 0. Prerequisites

- A Databricks Free Edition workspace.
- The Databricks CLI with bundle support (`databricks --version`; the bundle schema
  in this repo was checked against CLI v1.20.0).
- Python 3.10+ and this repo: `pip install -e ".[dev]"`.
- marketdata-lakehouse's `data/bronze` folder, already ingested and checked.

Authenticate once:

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

If you want to try a personal access token instead (a Day 0 check), create one under
Settings > Developer and run `databricks configure`.

## 1. Day 0 probe (one hour)

1. `databricks bundle deploy -t dev` syncs the repo, including `notebooks/00_capability_probe.py`.
   If the deploy itself fails, import just the notebook:
   `databricks workspace import --format SOURCE --language PYTHON notebooks/00_capability_probe.py /Users/<you>/00_capability_probe`.
2. Open the notebook on serverless compute and Run all.
3. Do the four manual checks listed at the end of the notebook, set them in `MANUAL`, re-run the last cells.
4. Copy the printed table into the README's Platform section. **Keep only PASS rows as claims.**

Decisions the probe makes for you:

| If this fails | Do this |
|---|---|
| create catalog | Keep `catalog=workspace` (the bundle default). |
| Auto Loader | Deploy with `--var ingest_mode=batch`, or `filestream` if that check passed. Don't claim Auto Loader. |
| declarative pipeline | Never deploy `-t dev_pipeline`. Don't claim pipeline expectations. |
| table tags | `docs_and_tags` reports the failure and carries on. Don't claim tags. |
| row filter | Expected to be skipped. Don't claim masking or row filters. |

## 2. Export the snapshot (on your machine)

Pick the bronze `ingest_date` whose reconciliation report you trust (for example
2026-09-08):

```powershell
python scripts/export_snapshot.py `
  --bronze-root "N:\AAA Projects\Financial projects\...\marketdata-lakehouse\data\bronze" `
  --ingest-date 2026-09-08 `
  --landing-root .\landing `
  --reconciliation-report "N:\...\marketdata-lakehouse\outputs\reconciliation_report_2026-09-08.txt"
```

It prints the three drop dates and rows per drop. Nothing is written to marketdata-lakehouse.
Keep the printed summary.

## 3. Deploy and set up

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev                      # add --var ingest_mode=batch if step 1 said so
databricks bundle run lakehouse_setup -t dev
```

`lakehouse_setup` creates `lakehouse_bronze`, `lakehouse_silver`, `lakehouse_gold`, `lakehouse_ops`
and the `landing` and `state` volumes in the `workspace` catalog.

## 4. Load drop by drop

For each drop date the export printed, oldest first:

```powershell
python scripts/upload_drop.py --drop-date <D> --landing-root .\landing `
  --volume dbfs:/Volumes/workspace/lakehouse_ops/landing
databricks bundle run lakehouse_daily -t dev --params run_date=<D>
```

Evidence: a screenshot of the green run graph for the last drop.

Then run once more with no new drop and no `run_date`. Every task should report
0 inserted and 0 updated, which is the idempotency evidence:

```powershell
databricks bundle run lakehouse_daily -t dev
```

## 5. One deliberate failure

Break the manifest, not the data, so nothing bad can be ingested:

```powershell
databricks fs cp dbfs:/Volumes/workspace/lakehouse_ops/landing/_manifests/drop_date=<D1>.json .\m.json
# edit one "rows" value in m.json
databricks fs cp .\m.json dbfs:/Volumes/workspace/lakehouse_ops/landing/_manifests/drop_date=<D1>.json --overwrite
databricks bundle run lakehouse_daily -t dev --params run_date=<D1>
```

Expected: the run fails at `verify_manifest` with no retry, and the failure email arrives.
Evidence: screenshots of the failed run and the email. Restore the original manifest afterwards
with `python scripts/upload_drop.py --drop-date <D1> ...`.

## 6. Delta evidence

```powershell
databricks bundle run lakehouse_delta_evidence -t dev
```

It writes `state/reports/delta_evidence.json`: row counts by version, schema enforcement and
evolution, OPTIMIZE file counts and query timings, VACUUM.

## 7. Governance

- `docs_and_tags` has already applied the comments, plus tags if supported. Its log lists anything that failed.
- Open `lakehouse_gold.fact_price_daily_consensus` in Catalog Explorer, then Lineage.
  Evidence: a screenshot of the lineage graph.

## 8. Optional: declarative pipeline

Only if the Day 0 pipeline check passed:

```powershell
databricks bundle deploy -t dev_pipeline
```

Start `lakehouse-dq-expectations` in the UI. Evidence: a screenshot of the expectations tab
showing pass and fail counts. Free Edition allows one active pipeline per type, so stop it afterwards.

## 9. Collect results for the README

```powershell
databricks fs cp -r dbfs:/Volumes/workspace/lakehouse_ops/state/reports .\outputs\reports
python scripts/render_results.py .\outputs\reports > .\outputs\results.md
```

Paste `results.md` into the README's Results section and the probe table into Platform.
Put the screenshots in `docs/screenshots/`.

## Teardown

`databricks bundle destroy -t dev` removes the jobs. Drop the schemas with
`DROP SCHEMA workspace.lakehouse_<layer> CASCADE` if you want the workspace clean.
