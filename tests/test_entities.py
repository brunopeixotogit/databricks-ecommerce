"""Tests for the user / product generators."""
from __future__ import annotations

from datetime import datetime, timezone

from src.simulator.entities import EntityGenerator


def test_users_are_deterministic_per_seed():
    g1 = EntityGenerator(seed=1)
    g2 = EntityGenerator(seed=1)
    a = [u["user_id"] for u in g1.generate_users(20, datetime(2024, 1, 1, tzinfo=timezone.utc))]
    b = [u["user_id"] for u in g2.generate_users(20, datetime(2024, 1, 1, tzinfo=timezone.utc))]
    assert a == b


def test_users_have_required_fields():
    g = EntityGenerator(seed=2)
    users = list(g.generate_users(5, datetime(2024, 1, 1, tzinfo=timezone.utc)))
    for u in users:
        assert u["user_id"].startswith("u_")
        assert "@" in u["email"]
        assert u["loyalty_tier"] in {"bronze", "silver", "gold", "platinum"}
        assert u["signup_ts"].tzinfo is not None


def test_products_respect_category_and_price_constraints():
    g = EntityGenerator(seed=3)
    cats = {"electronics": 0.5, "appliances": 0.3, "furniture": 0.2}
    prices = {
        "electronics": [10.0, 1000.0],
        "appliances":  [50.0, 2000.0],
        "furniture":   [20.0, 1500.0],
    }
    products = list(g.generate_products(200, cats, prices))
    assert len(products) == 200
    assert {p["category"] for p in products} <= set(cats.keys())
    for p in products:
        lo, hi = prices[p["category"]]
        assert lo <= p["price"] <= hi
        assert p["sku"].startswith(p["category"][:3].upper())
