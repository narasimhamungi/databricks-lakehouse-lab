"""One SparkSession for the whole test run.

It tries to start with Delta Lake (delta-spark pulls its jars from Maven
Central). If that fails, the session falls back to plain Spark and tests
marked ``delta`` are skipped, unless REQUIRE_DELTA=1, which CI sets so a
missing Delta can never pass silently.
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile

import pytest

from lakehouse.config import LakehouseConfig

_STATE: dict = {}


def _start():
    if "spark" in _STATE:
        return _STATE["spark"]
    from lakehouse.spark import get_spark

    warehouse = tempfile.mkdtemp(prefix="lakehouse-warehouse-")
    conf = {"spark.sql.warehouse.dir": warehouse}
    try:
        spark = get_spark("tests", with_delta=True, conf=conf)
        probe = os.path.join(warehouse, "_delta_probe")
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
        _STATE["delta"] = True
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("REQUIRE_DELTA") == "1":
            raise
        _STATE["delta"] = False
        _STATE["delta_error"] = str(exc).splitlines()[0][:200]
        spark = get_spark("tests", with_delta=False, conf=conf)
    spark.sparkContext.setLogLevel("ERROR")
    _STATE["spark"] = spark
    _STATE["warehouse"] = warehouse
    return spark


@pytest.fixture(scope="session")
def spark():
    s = _start()
    yield s


def pytest_collection_modifyitems(config, items):
    _start()
    if _STATE.get("delta"):
        return
    reason = f"Delta Lake not available: {_STATE.get('delta_error', 'unknown')}"
    for item in items:
        if "delta" in item.keywords:
            item.add_marker(pytest.mark.skip(reason=reason))


def pytest_sessionfinish(session, exitstatus):
    if "spark" in _STATE:
        _STATE["spark"].stop()
        shutil.rmtree(_STATE.get("warehouse", ""), ignore_errors=True)


@pytest.fixture
def local_cfg(tmp_path) -> LakehouseConfig:
    return LakehouseConfig(
        catalog=None,
        schema_prefix="t_" + re.sub(r"\W", "_", tmp_path.name)[-24:] + "_",
        landing_root=str(tmp_path / "landing"),
        state_root=str(tmp_path / "state"),
        ingest_mode="filestream",
    )
