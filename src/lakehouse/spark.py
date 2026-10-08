"""Getting a SparkSession that can read and write Delta.

On Databricks the session already exists (serverless jobs run on Spark
Connect, so nothing here touches sparkContext or RDDs). Locally, Delta has to
be wired in through delta-spark, which downloads its jars from Maven Central
the first time.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from lakehouse.config import on_databricks


def get_spark(app_name: str = "lakehouse", with_delta: bool = True, conf: dict | None = None) -> SparkSession:
    if on_databricks():
        return SparkSession.builder.getOrCreate()

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    if not with_delta:
        return builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
