"""The open-source file-stream path that bronze uses locally (and the logic
Auto Loader replaces on Databricks): an available-now trigger with a
checkpoint must pick up each landing file exactly once. Tested with a parquet
sink so it runs without the Delta jars."""

from __future__ import annotations

import os
import shutil

from lakehouse import bronze, export
from lakehouse.config import LakehouseConfig
from lakehouse.schemas import YFINANCE_PRICES
from tests import factory


def _run_once(spark, cfg, sink):
    stream = bronze.with_ingest_metadata(bronze._stream_reader(spark, cfg, "yfinance_prices", YFINANCE_PRICES))
    q = (
        stream.writeStream.format("parquet")
        .option("path", sink)
        .option("checkpointLocation", cfg.checkpoint("yf_test"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    return spark.read.parquet(sink).count() if os.path.isdir(sink) else 0


def test_available_now_stream_reads_each_file_exactly_once(spark, tmp_path):
    staging = str(tmp_path / "staging")
    summary = export.export_snapshot(factory.write_mdl_bronze(str(tmp_path / "mdl")), factory.INGEST_DATE, staging)
    landing = tmp_path / "landing"
    cfg = LakehouseConfig(None, "s_", str(landing), str(tmp_path / "state"))
    sink = str(tmp_path / "sink")

    counts = []
    for d in summary["drop_dates"]:
        src = os.path.join(staging, "yfinance_prices", f"drop_date={d}")
        shutil.copytree(src, landing / "yfinance_prices" / f"drop_date={d}")
        counts.append(_run_once(spark, cfg, sink))
    rerun = _run_once(spark, cfg, sink)

    n = len(factory.TRADING_DAYS)
    assert counts == [3 * (n - 2) + 1, 3 * (n - 1) + 1, 3 * n + 1 + 3]  # +1 planted duplicate, +3 overlap day
    assert rerun == counts[-1]
    drops = {str(r[0]) for r in spark.read.parquet(sink).select("_drop_date").distinct().collect()}
    assert drops == set(summary["drop_dates"])
