"""Lakeflow declarative pipeline: the quality rules as pipeline expectations.

The same Rule objects the job path uses (lakehouse.quality), so the two
cannot drift: "drop" rules become expect_or_drop, "warn" rules become expect
(the row is kept and the violation counted in the pipeline event log).

Optional. Deployed only with the dev_pipeline bundle target, and claimed in
the README only if the Day 0 probe shows pipelines run on the workspace.
"""

import sys

try:
    from pyspark import pipelines as dp
except ImportError:  # runtimes that predate pyspark.pipelines
    import dlt as dp

spark = globals()["spark"]  # provided by the pipeline runtime

_src = spark.conf.get("lakehouse.src_root", "")
if _src and _src not in sys.path:
    sys.path.insert(0, _src)

from lakehouse.quality import PRICE_RULES  # noqa: E402
from lakehouse.silver import type_prices  # noqa: E402

CATALOG = spark.conf.get("lakehouse.catalog")
PREFIX = spark.conf.get("lakehouse.schema_prefix", "lakehouse_")
_view = getattr(dp, "materialized_view", None) or dp.table


def _expect(fn, rules):
    for rule in rules:
        decorator = dp.expect_or_drop if rule.severity == "drop" else dp.expect
        fn = decorator(rule.name, rule.sql)(fn)
    return fn


def _checked(source: str, name: str):
    # Names are new on purpose: the first pipeline's dq_<source>_prices_checked
    # views outlived it and stay owned by the deleted pipeline.
    def build():
        return type_prices(spark.read.table(f"{CATALOG}.{PREFIX}bronze.{source}"))

    build.__name__ = name
    return _view(
        name=name,
        comment=f"{source} bronze after the job path's quality rules, applied as expectations",
    )(_expect(build, PRICE_RULES))


dq_yfinance_expectations = _checked("yfinance_prices", "dq_yfinance_expectations")
dq_tiingo_expectations = _checked("tiingo_prices", "dq_tiingo_expectations")
