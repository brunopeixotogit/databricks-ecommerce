"""Tests for src.simulator.run — orchestration glue.

Pure-Python tests. We do not exercise the file emitters here (those are
covered indirectly by integration runs); we focus on behavioural
guarantees that should never regress.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.common.config import load_config
from src.simulator.run import (
    LandingPaths,
    bootstrap_entities,
    make_session_stream,
)


def test_landing_paths_from_root():
    paths = LandingPaths.from_root("/Volumes/main/ecom_bronze/landing")
    assert paths.users    == "/Volumes/main/ecom_bronze/landing/users"
    assert paths.products == "/Volumes/main/ecom_bronze/landing/products"
    assert paths.events   == "/Volumes/main/ecom_bronze/landing/events"


def test_bootstrap_entities_is_deterministic():
    """Same seed → identical populations. Backfills must be reproducible."""
    cfg = load_config("simulator")
    fixed_now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    u1, p1 = bootstrap_entities(cfg, now=fixed_now)
    u2, p2 = bootstrap_entities(cfg, now=fixed_now)

    assert [u["user_id"] for u in u1] == [u["user_id"] for u in u2]
    assert [p["product_id"] for p in p1] == [p["product_id"] for p in p2]


def test_bootstrap_entities_respects_population_sizes():
    cfg = load_config("simulator")
    users, products = bootstrap_entities(cfg)
    assert len(users)    == cfg["users"]["initial_population"]
    assert len(products) == cfg["products"]["catalog_size"]


def test_session_stream_yields_events():
    """Stream must yield at least one event per requested session and
    every event must conform to the producer contract (event_id present,
    valid event_type, schema_version stamped)."""
    cfg = load_config("simulator")
    users, products = bootstrap_entities(cfg)
    stream = make_session_stream(users, products, cfg)

    events = list(stream(5))
    assert len(events) >= 5

    valid_types = {"page_view", "add_to_cart", "purchase", "abandon_cart"}
    for ev in events:
        assert ev.get("event_id"), "event_id must be present"
        assert ev["event_type"] in valid_types
        assert ev.get("schema_version") == "1.0.0"
