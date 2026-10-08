"""Names and paths, in one place.

The same code runs in two places: a Databricks workspace (Unity Catalog
three-part names, a volume for landing files and checkpoints) and a local
Spark session for tests (two-part names in the session catalog, a temp
directory). ``LakehouseConfig`` is the only thing that differs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Mirrors marketdata-lakehouse src/config.py. Kept identical on purpose so the
# port can be checked against the original pipeline's outputs.
RECONCILIATION_TOLERANCE = 0.005  # 0.5% symmetric adj_close difference
OHLC_TOLERANCE = 0.9999  # share of rows that must satisfy OHLC ordering
DIM_DATE_START = "2019-01-01"  # DEFAULT_START_DATE in the original

PRICE_SOURCES = ("yfinance", "tiingo")
LANDING_SOURCES = ("yfinance_prices", "tiingo_prices", "constituents", "openfigi_mapping", "fred_series")

LAYERS = ("bronze", "silver", "gold", "ops")


@dataclass(frozen=True)
class LakehouseConfig:
    """Where tables and files live.

    catalog       Unity Catalog catalog, or None for a local session catalog.
    schema_prefix Prefix for the per-layer schemas: "<prefix>bronze" and so on.
    landing_root  Directory holding <source>/drop_date=YYYY-MM-DD/*.parquet
                  and _manifests/drop_date=YYYY-MM-DD.json.
    state_root    Directory for streaming checkpoints and Auto Loader schemas.
    ingest_mode   "autoloader" (Databricks only), "filestream" (open-source
                  Structured Streaming file source) or "batch".
    """

    catalog: str | None
    schema_prefix: str
    landing_root: str
    state_root: str
    ingest_mode: str = "filestream"

    def __post_init__(self) -> None:
        if self.ingest_mode not in ("autoloader", "filestream", "batch"):
            raise ValueError(f"ingest_mode must be autoloader, filestream or batch, got {self.ingest_mode!r}")

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")
        name = f"{self.schema_prefix}{layer}"
        return f"{self.catalog}.{name}" if self.catalog else name

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    def landing(self, source: str) -> str:
        if source not in LANDING_SOURCES:
            raise ValueError(f"Unknown landing source {source!r}")
        return f"{self.landing_root.rstrip('/')}/{source}"

    @property
    def manifests_dir(self) -> str:
        return f"{self.landing_root.rstrip('/')}/_manifests"

    @property
    def reference_dir(self) -> str:
        return f"{self.landing_root.rstrip('/')}/_reference"

    def checkpoint(self, name: str) -> str:
        return f"{self.state_root.rstrip('/')}/checkpoints/{name}"

    def autoloader_schema(self, name: str) -> str:
        return f"{self.state_root.rstrip('/')}/schemas/{name}"


def databricks_config(
    catalog: str, schema_prefix: str = "lakehouse_", ingest_mode: str = "autoloader"
) -> LakehouseConfig:
    """Config for a Unity Catalog workspace. Landing files and state live in
    volumes created by jobs/setup.py."""
    landing_schema = f"{schema_prefix}ops"
    return LakehouseConfig(
        catalog=catalog,
        schema_prefix=schema_prefix,
        landing_root=f"/Volumes/{catalog}/{landing_schema}/landing",
        state_root=f"/Volumes/{catalog}/{landing_schema}/state",
        ingest_mode=ingest_mode,
    )


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ
