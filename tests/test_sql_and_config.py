from __future__ import annotations

import pytest

from lakehouse import cli, delta_io
from lakehouse.config import LakehouseConfig, databricks_config
from lakehouse.tasks import parity_check


def test_merge_sql_updates_only_changed_rows_and_respects_the_guard():
    sql = delta_io.build_merge_sql(
        "silver.p",
        "v",
        ("ticker", "date"),
        ("ticker", "date", "close", "_drop_date"),
        compare=("close",),
        guard="s._drop_date >= t._drop_date",
    )
    assert "ON t.`ticker` = s.`ticker` AND t.`date` = s.`date`" in sql
    assert "WHEN MATCHED AND (s._drop_date >= t._drop_date) AND (NOT (t.`close` <=> s.`close`))" in sql
    assert "t.`_drop_date` = s.`_drop_date`" in sql  # lineage moves with the value
    assert "INSERT (`ticker`, `date`, `close`, `_drop_date`)" in sql


def test_merge_sql_compares_every_non_key_column_by_default():
    sql = delta_io.build_merge_sql("g.d", "v", ("k",), ("k", "a", "b"))
    assert "NOT (t.`a` <=> s.`a`) OR NOT (t.`b` <=> s.`b`)" in sql


def test_scd2_merge_sql_shape():
    sql = delta_io.build_scd2_merge_sql("g.dim_security", "stage", ("company_name", "figi"))
    assert "ON t.ticker = s.merge_key AND t.is_current = true" in sql
    assert (
        "WHEN MATCHED AND s.action = 'close' THEN UPDATE SET t.effective_to = s.snapshot_date, t.is_current = false"
        in sql
    )
    assert "WHEN NOT MATCHED AND s.action = 'insert'" in sql


def test_names_local_and_unity_catalog():
    local = LakehouseConfig(None, "lh_", "/tmp/l", "/tmp/s")
    assert local.table("gold", "dim_date") == "lh_gold.dim_date"
    uc = databricks_config("workspace")
    assert uc.table("silver", "prices_yfinance") == "workspace.lakehouse_silver.prices_yfinance"
    assert uc.landing("fred_series") == "/Volumes/workspace/lakehouse_ops/landing/fred_series"
    assert uc.checkpoint("bronze_x") == "/Volumes/workspace/lakehouse_ops/state/checkpoints/bronze_x"
    assert uc.ingest_mode == "autoloader"


def test_config_rejects_unknown_layer_and_mode():
    with pytest.raises(ValueError):
        LakehouseConfig(None, "x_", "/a", "/b", ingest_mode="kafka")
    with pytest.raises(ValueError):
        LakehouseConfig(None, "x_", "/a", "/b").table("platinum", "t")


def test_cli_local_and_databricks_configs(tmp_path):
    args = cli.parse_args("bronze", ["--local-root", str(tmp_path), "--run-date", "2024-01-11"])
    cfg = cli.build_config(args)
    assert cfg.catalog is None and cfg.ingest_mode == "filestream" and args.run_date == "2024-01-11"
    args = cli.parse_args("bronze", ["--catalog", "workspace", "--ingest-mode", "batch", "--unknown-extra", "x"])
    assert cli.build_config(args).ingest_mode == "batch"
    with pytest.raises(SystemExit):
        cli.build_config(cli.parse_args("bronze", []))


def test_parity_check_reports_each_difference():
    actual = {"compared": 10, "flagged": 2, "yfinance_only": 5, "tiingo_only": 1}
    assert parity_check(actual, {**actual, "tolerance": 0.005}) == []
    problems = parity_check(actual, {**actual, "flagged": 3, "tolerance": 0.01})
    assert any("tolerance" in p for p in problems) and any("flagged: reference 3" in p for p in problems)


class _FakeSpark:
    """Just enough of SparkSession.sql for ensure_catalog."""

    def __init__(self, existing, fail_create=False):
        self.existing, self.fail_create, self.calls = set(existing), fail_create, []

    def sql(self, statement):
        self.calls.append(statement)
        if statement.startswith("SHOW CATALOGS LIKE"):
            name = statement.split("'")[1]
            return type("R", (), {"count": lambda _self: int(name in self.existing)})()
        if self.fail_create:
            raise PermissionError("PERMISSION_DENIED")
        self.existing.add(statement.split()[-1])


def test_ensure_catalog_creates_only_when_missing():
    from lakehouse.tasks import ensure_catalog

    spark = _FakeSpark({"workspace"})
    assert ensure_catalog(spark, "workspace") is False
    assert not any(c.startswith("CREATE") for c in spark.calls)
    assert ensure_catalog(spark, "lakehouse_lab") is True
    assert "CREATE CATALOG IF NOT EXISTS lakehouse_lab" in spark.calls


def test_ensure_catalog_names_the_fallback_when_create_is_blocked():
    from lakehouse.tasks import ensure_catalog

    with pytest.raises(RuntimeError, match="--var catalog=workspace"):
        ensure_catalog(_FakeSpark(set(), fail_create=True), "lakehouse_lab")
