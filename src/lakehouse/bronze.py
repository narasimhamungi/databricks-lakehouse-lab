"""Bronze: landing parquet -> append-only Delta tables, one per source, with
where-and-when metadata on every row.

Three ways in, chosen by ``LakehouseConfig.ingest_mode``:
- autoloader  Databricks Auto Loader (cloudFiles) with an available-now
              trigger. Tracks processed files in its checkpoint, and evolves
              the schema when a new column appears.
- filestream  The open-source Structured Streaming file source, same trigger
              and checkpoint semantics. This is what runs in local tests.
- batch       A plain read that skips files already present in bronze, for
              a workspace where streaming is not available.
All three are idempotent: re-running over the same landing files adds 0 rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lakehouse.config import LakehouseConfig

DROP_DATE_PATTERN = r"drop_date=(\d{4}-\d{2}-\d{2})"


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    """Add _source_file, _file_modified_at, _drop_date and _ingested_at.

    Uses the file-source ``_metadata`` column rather than input_file_name(),
    which Unity Catalog does not support. A file outside a drop_date=
    directory gets a NULL _drop_date, which ingest() treats as an error.
    """
    if "drop_date" in df.columns:  # Hive-style partition inference, if any reader did it
        df = df.drop("drop_date")
    extracted = F.regexp_extract(F.col("_metadata.file_path"), DROP_DATE_PATTERN, 1)
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_drop_date", F.when(extracted != "", F.to_date(extracted)))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def new_files_only(landed: DataFrame, already_ingested_files: DataFrame) -> DataFrame:
    """Batch-mode idempotency: keep rows whose file is not yet in bronze."""
    seen = already_ingested_files.select("_source_file").distinct()
    return landed.join(seen, on="_source_file", how="left_anti")


def _stream_reader(spark: SparkSession, cfg: LakehouseConfig, source: str, schema: StructType):
    path = cfg.landing(source)
    if cfg.ingest_mode == "autoloader":
        # No .schema() here: Auto Loader infers it from the parquet footers
        # (which carry the landing contract's types) and stores it at
        # schemaLocation, so a new column evolves the table instead of being
        # dropped. On that event the stream stops once and the task retry
        # restarts it with the wider schema.
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", cfg.autoloader_schema(source))
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.partitionColumns", "")
            .load(path)
        )
    return spark.readStream.schema(schema).option("recursiveFileLookup", "true").parquet(path)


def ingest(spark: SparkSession, cfg: LakehouseConfig, source: str, schema: StructType, target: str) -> dict:
    """Load whatever has landed for ``source`` and not been loaded yet.
    Returns row counts before and after so each run's addition is on record."""
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0

    if cfg.ingest_mode == "batch":
        landed = with_ingest_metadata(
            spark.read.schema(schema).option("recursiveFileLookup", "true").parquet(cfg.landing(source))
        )
        if spark.catalog.tableExists(target):
            landed = new_files_only(landed, spark.table(target))
        landed.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    else:
        stream = with_ingest_metadata(_stream_reader(spark, cfg, source, schema))
        query = (
            stream.writeStream.format("delta")
            .option("checkpointLocation", cfg.checkpoint(f"bronze_{source}"))
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()

    after = spark.table(target).count()
    orphans = spark.table(target).where(F.col("_drop_date").isNull()).count()
    if orphans:
        raise ValueError(
            f"{target}: {orphans} rows came from files outside a drop_date=YYYY-MM-DD directory. "
            "Fix the landing layout; bronze rows must be traceable to a drop."
        )
    return {"table": target, "rows_before": before, "rows_after": after, "rows_added": after - before}
