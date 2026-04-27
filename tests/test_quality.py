"""Tests for the quality framework. These do not require Spark — we
fake a minimal DataFrame-like with ``filter`` + ``count``."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from src.common.quality import Expectation, enforce, evaluate


@dataclass
class FakeDF:
    rows: List[dict]

    def filter(self, expr: str):
        # extremely small SQL subset: "NOT (col IS NOT NULL)" / "col >= 0"
        # tests use simple expressions only.
        if expr.startswith("NOT ("):
            inner = expr[5:-1]
            keep = lambda r: not _eval(inner, r)
        else:
            keep = lambda r: _eval(expr, r)
        return FakeDF([r for r in self.rows if keep(r)])

    def count(self) -> int:
        return len(self.rows)


def _eval(expr: str, row: dict) -> bool:
    expr = expr.strip()
    if expr.endswith("IS NOT NULL"):
        return row.get(expr.split()[0]) is not None
    if ">=" in expr:
        col, val = [s.strip() for s in expr.split(">=")]
        v = row.get(col)
        return v is not None and v >= float(val)
    raise ValueError(f"unsupported test expr: {expr}")


def test_evaluate_reports_failing_rows():
    df = FakeDF([
        {"event_id": "a", "price": 10},
        {"event_id": None, "price": 5},
        {"event_id": "c", "price": -1},
    ])
    results = evaluate(df, [
        Expectation("event_id_not_null", "event_id IS NOT NULL", "fail"),
        Expectation("non_neg_price", "price >= 0", "warn"),
    ])
    by_name = {r["expectation"]: r for r in results}
    assert by_name["event_id_not_null"]["failing"] == 1
    assert by_name["non_neg_price"]["failing"] == 1
    assert by_name["event_id_not_null"]["total"] == 3


def test_enforce_raises_only_on_fail_severity():
    results = [
        {"expectation": "x", "severity": "warn", "failing": 5, "total": 10},
    ]
    enforce(results)  # warn-only — no raise

    results = [
        {"expectation": "y", "severity": "fail", "failing": 1, "total": 10},
    ]
    with pytest.raises(AssertionError):
        enforce(results)
