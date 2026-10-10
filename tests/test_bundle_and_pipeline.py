"""Static checks on the deployable pieces: the bundle and the pipeline source.
(The bundle is also validated against the Databricks CLI's JSON schema by
hand; see docs/RUNBOOK.md.)"""

from __future__ import annotations

import ast
import os
import re

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _bundle():
    with open(os.path.join(ROOT, "databricks.yml"), encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _daily():
    return _bundle()["resources"]["jobs"]["lakehouse_daily"]


def test_daily_job_runs_the_tasks_in_order():
    tasks = {t["task_key"]: t for t in _daily()["tasks"]}
    order = ["verify_manifest", "bronze", "silver", "gold", "docs_and_tags", "dq_report"]
    assert list(tasks) == order
    for prev, nxt in zip(order, order[1:], strict=False):
        assert tasks[nxt]["depends_on"] == [{"task_key": prev}]


def test_retries_only_on_tasks_that_can_fail_transiently():
    retries = {t["task_key"]: t.get("max_retries", 0) for t in _daily()["tasks"]}
    assert retries == {"verify_manifest": 0, "bronze": 2, "silver": 2, "gold": 2, "docs_and_tags": 2, "dq_report": 0}


def test_no_retry_tasks_also_disable_serverless_auto_optimization():
    # Serverless auto-optimization retries failed tasks regardless of
    # max_retries; at-most-once tasks must switch it off explicitly.
    for t in _daily()["tasks"]:
        assert t.get("disable_auto_optimization", False) is (t.get("max_retries", 0) == 0), t["task_key"]


def test_failure_email_and_run_date_parameter():
    job = _daily()
    assert job["email_notifications"]["on_failure"] == ["${var.failure_email}"]
    assert {"name": "run_date", "default": ""} in job["parameters"]
    assert job["max_concurrent_runs"] == 1


def test_every_task_points_at_an_existing_entry_point_with_shared_args():
    bundle = _bundle()
    seen = set()
    for job in bundle["resources"]["jobs"].values():
        for t in job["tasks"]:
            spt = t["spark_python_task"]
            assert os.path.isfile(os.path.join(ROOT, spt["python_file"])), spt["python_file"]
            params = spt["parameters"]
            assert "--run-date={{job.parameters.run_date}}" in params
            assert "--src-root=${workspace.file_path}/src" in params
            seen.add(tuple(params))
    assert len(seen) == 1  # one shared argument list (YAML anchor)


def test_every_referenced_job_parameter_is_declared_on_its_job():
    # The Jobs API rejects {{job.parameters.X}} when the job has no parameter X
    # (400 INVALID_PARAMETER_VALUE at deploy); the CLI's schema check does not.
    bundle = _bundle()
    jobs = dict(bundle["resources"]["jobs"])
    for target in bundle["targets"].values():
        jobs.update((target.get("resources") or {}).get("jobs") or {})
    for key, job in jobs.items():
        declared = {p["name"] for p in job.get("parameters", [])}
        for t in job["tasks"]:
            for arg in (t.get("spark_python_task") or {}).get("parameters", []):
                for ref in re.findall(r"\{\{job\.parameters\.(\w+)\}\}", arg):
                    assert ref in declared, f"{key}.{t['task_key']} references undeclared job parameter {ref}"


def test_pipeline_only_in_its_own_target():
    bundle = _bundle()
    assert "pipelines" not in bundle["resources"]
    pipe = bundle["targets"]["dev_pipeline"]["resources"]["pipelines"]["lakehouse_dq_expectations"]
    assert pipe["serverless"] is True
    assert os.path.isfile(os.path.join(ROOT, pipe["libraries"][0]["file"]["path"]))


def test_dq_evidence_job_runs_the_pipeline_then_reads_its_event_log():
    res = _bundle()["targets"]["dev_pipeline"]["resources"]
    # No published event-log table: it would outlive a destroyed pipeline and
    # block the next deploy (TABLE_OR_VIEW_ALREADY_EXISTS).
    assert "event_log" not in res["pipelines"]["lakehouse_dq_expectations"]
    tasks = {t["task_key"]: t for t in res["jobs"]["lakehouse_dq_evidence"]["tasks"]}
    pipeline_id = tasks["run_pipeline"]["pipeline_task"]["pipeline_id"]
    assert pipeline_id == "${resources.pipelines.lakehouse_dq_expectations.id}"
    assert tasks["pipeline_evidence"]["depends_on"] == [{"task_key": "run_pipeline"}]
    assert os.path.isfile(os.path.join(ROOT, tasks["pipeline_evidence"]["spark_python_task"]["python_file"]))


def test_pipeline_source_uses_the_shared_rules():
    with open(os.path.join(ROOT, "pipelines", "dq_expectations.py"), encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    imported = {(n.module, a.name) for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    assert ("lakehouse.quality", "PRICE_RULES") in imported


def test_probe_notebook_is_a_databricks_source_notebook():
    with open(os.path.join(ROOT, "notebooks", "00_capability_probe.py"), encoding="utf-8") as fh:
        text = fh.read()
    assert text.startswith("# Databricks notebook source")
    ast.parse(text)  # valid Python
