"""Tests for the session-level behavior engine."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.simulator.behavior import (
    BehaviorEngine,
    Product,
    SessionContext,
    diurnal_factor,
)


def _ctx(user_id="u_test"):
    return SessionContext(
        session_id="s_test",
        user_id=user_id,
        start_ts=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        device="desktop",
        country="US",
        user_agent="ua",
        ip="1.2.3.4",
        referrer="google",
    )


def _catalog():
    return [
        Product("p1", "electronics", 100.0),
        Product("p2", "appliances", 250.0),
        Product("p3", "furniture", 75.0),
    ]


def test_purchase_path_emits_full_funnel():
    cfg = {
        "page_views_per_session_lambda": 3.0,
        "add_to_cart_probability": 1.0,
        "checkout_given_cart": 1.0,
        "purchase_given_checkout": 1.0,
        "abandonment_idle_minutes": 30,
    }
    engine = BehaviorEngine(cfg, seed=7)
    events = list(engine.simulate_session(_ctx(), _catalog()))
    types = {e["event_type"] for e in events}
    assert {"page_view", "add_to_cart", "purchase"} <= types
    assert "abandon_cart" not in types
    # purchase events share an order_id
    order_ids = {e["order_id"] for e in events if e["event_type"] == "purchase"}
    assert len(order_ids) == 1
    assert next(iter(order_ids)) is not None


def test_browse_only_session_has_no_cart_events():
    cfg = {
        "page_views_per_session_lambda": 2.0,
        "add_to_cart_probability": 0.0,
        "checkout_given_cart": 1.0,
        "purchase_given_checkout": 1.0,
        "abandonment_idle_minutes": 30,
    }
    engine = BehaviorEngine(cfg, seed=1)
    events = list(engine.simulate_session(_ctx(), _catalog()))
    assert all(e["event_type"] == "page_view" for e in events)


def test_cart_abandonment_path():
    cfg = {
        "page_views_per_session_lambda": 3.0,
        "add_to_cart_probability": 1.0,
        "checkout_given_cart": 0.0,         # always abandon before checkout
        "purchase_given_checkout": 1.0,
        "abandonment_idle_minutes": 30,
    }
    engine = BehaviorEngine(cfg, seed=2)
    events = list(engine.simulate_session(_ctx(), _catalog()))
    types = [e["event_type"] for e in events]
    assert "add_to_cart" in types
    assert "abandon_cart" in types
    assert "purchase" not in types


def test_anonymous_session_has_null_user_id():
    cfg = {
        "page_views_per_session_lambda": 2.0,
        "add_to_cart_probability": 0.0,
        "checkout_given_cart": 1.0,
        "purchase_given_checkout": 1.0,
        "abandonment_idle_minutes": 30,
    }
    engine = BehaviorEngine(cfg, seed=3)
    events = list(engine.simulate_session(_ctx(user_id=None), _catalog()))
    assert events
    assert all(e["user_id"] is None for e in events)


def test_empty_catalog_emits_nothing():
    cfg = {
        "page_views_per_session_lambda": 3.0,
        "add_to_cart_probability": 1.0,
        "checkout_given_cart": 1.0,
        "purchase_given_checkout": 1.0,
        "abandonment_idle_minutes": 30,
    }
    engine = BehaviorEngine(cfg, seed=0)
    assert list(engine.simulate_session(_ctx(), [])) == []


@pytest.mark.parametrize("hour", range(0, 24))
def test_diurnal_factor_in_expected_range(hour):
    assert 0.39 < diurnal_factor(hour) <= 1.0
