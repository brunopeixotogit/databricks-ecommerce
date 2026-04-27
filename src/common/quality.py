"""Lightweight data-quality expectations for non-DLT pipelines.

Each expectation is a SQL predicate that should evaluate to TRUE for a
valid row. ``evaluate`` returns counts and pass-rates; ``enforce``
raises if any ``fail``-severity expectation has any failing row.

In a production setup with Delta Live Tables, prefer DLT expectations
which integrate with lineage and the event log. This helper exists for
plain PySpark pipelines (Free Edition).
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pyspark is only needed for type hints; pure-Python at runtime
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class Expectation:
    name: str
    predicate: str          # SQL expression — TRUE means valid row
    severity: str = "warn"  # 'warn' or 'fail'


def evaluate(df: DataFrame, expectations: Iterable[Expectation]) -> list[dict]:
    """Run every expectation and return a structured result list."""
    results: list[dict] = []
    total = df.count()
    for exp in expectations:
        bad = df.filter(f"NOT ({exp.predicate})").count()
        results.append({
            "expectation": exp.name,
            "predicate": exp.predicate,
            "severity": exp.severity,
            "total": total,
            "failing": bad,
            "pass_rate": (total - bad) / total if total else 1.0,
        })
    return results


def enforce(results: Iterable[dict]) -> None:
    """Raise if any failing row exists for a ``fail``-severity expectation."""
    failures = [r for r in results if r["severity"] == "fail" and r["failing"] > 0]
    if failures:
        raise AssertionError(
            "Quality gate failed: "
            + ", ".join(f"{r['expectation']} ({r['failing']}/{r['total']})" for r in failures)
        )
