"""Pytest configuration.

Two responsibilities:

1. Make ``src.*`` importable without installing the package
   (so ``pip install -e .`` is optional for quick local runs).
2. Provide an optional, session-scoped ``spark`` fixture.
   Tests that need a real SparkSession opt in by adding ``spark`` to
   their argument list. If PySpark is not installed (the default in
   CI's pure-Python lane) the fixture is **skipped**, never errored —
   so the same test file works locally with the ``[spark]`` extra and
   in a stripped-down CI job alike.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """Minimal local SparkSession for tests marked ``@pytest.mark.spark``.

    Skipped (not failed) when PySpark is unavailable, so CI can run the
    pure-Python suite without installing PySpark's ~300 MB footprint.
    Install PySpark locally with ``pip install -e .[spark]``.
    """
    pytest.importorskip(
        "pyspark",
        reason="PySpark not installed — install with `pip install -e .[spark]`",
    )
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("databricks-ecommerce-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
