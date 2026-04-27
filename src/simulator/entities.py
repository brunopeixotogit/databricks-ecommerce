"""Generators for the simulated user population and product catalog.

These are *populations*, not events. Bootstrap once and then mutate
over time (signups, profile updates, price changes, deactivations).
The generator is deterministic given the same seed, which makes
backfills reproducible.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Mapping, Sequence

from faker import Faker

from src.common.schemas import SCHEMA_VERSION

CATEGORIES: List[str] = ["electronics", "appliances", "furniture"]

SUBCATEGORIES: Dict[str, List[str]] = {
    "electronics": ["smartphone", "laptop", "tv", "headphones", "camera", "tablet", "smartwatch"],
    "appliances":  ["refrigerator", "washer", "dryer", "oven", "microwave", "dishwasher", "vacuum"],
    "furniture":   ["sofa", "bed", "table", "chair", "wardrobe", "desk", "bookshelf"],
}

BRANDS: Dict[str, List[str]] = {
    "electronics": ["Acme", "Nimbus", "Volta", "Pixelon", "Hertz", "Quanta"],
    "appliances":  ["Coldwave", "Hearthline", "Kettlewise", "Polaris", "Frostpeak"],
    "furniture":   ["Oakhaus", "Norden", "LoomCo", "Pinecrest", "Linden"],
}

LOYALTY_TIERS: List[str] = ["bronze", "silver", "gold", "platinum"]


class EntityGenerator:
    """Deterministic generator for users and products."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.faker = Faker()
        Faker.seed(seed)

    # ------------------------------------------------------------------ users
    def generate_users(
        self,
        n: int,
        signup_start: datetime,
        marketing_opt_in_rate: float = 0.6,
        loyalty_distribution: Mapping[str, float] | None = None,
    ) -> Iterator[dict]:
        loy_dist = loyalty_distribution or {
            "bronze": 0.60, "silver": 0.25, "gold": 0.12, "platinum": 0.03
        }
        loy_keys = list(loy_dist.keys())
        loy_weights = [loy_dist[k] for k in loy_keys]
        for _ in range(n):
            uid = f"u_{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12]}"
            ts = self.faker.date_time_between(
                start_date=signup_start,
                end_date="now",
                tzinfo=timezone.utc,
            )
            yield {
                "user_id": uid,
                "email": self.faker.email(),
                "first_name": self.faker.first_name(),
                "last_name": self.faker.last_name(),
                "country": self.faker.country_code(),
                "city": self.faker.city(),
                "signup_ts": ts,
                "marketing_opt_in": self.rng.random() < marketing_opt_in_rate,
                "loyalty_tier": self.rng.choices(loy_keys, weights=loy_weights)[0],
                "updated_ts": ts,
                "schema_version": SCHEMA_VERSION,
            }

    # --------------------------------------------------------------- products
    def generate_products(
        self,
        n: int,
        category_weights: Mapping[str, float],
        price_ranges: Mapping[str, Sequence[float]],
        active_rate: float = 0.97,
    ) -> Iterator[dict]:
        cats = list(category_weights.keys())
        weights = [category_weights[c] for c in cats]
        now = datetime.now(timezone.utc)
        for _ in range(n):
            cat = self.rng.choices(cats, weights=weights)[0]
            sub = self.rng.choice(SUBCATEGORIES[cat])
            brand = self.rng.choice(BRANDS[cat])
            lo, hi = price_ranges[cat]
            price = round(self.rng.uniform(float(lo), float(hi)), 2)
            pid = f"p_{uuid.UUID(int=self.rng.getrandbits(128)).hex[:10]}"
            yield {
                "product_id": pid,
                "sku": f"{cat[:3].upper()}-{sub[:3].upper()}-{self.rng.randint(10000, 99999)}",
                "name": f"{brand} {sub.title()} {self.rng.randint(100, 999)}",
                "category": cat,
                "subcategory": sub,
                "brand": brand,
                "price": price,
                "currency": "USD",
                "active": self.rng.random() < active_rate,
                "updated_ts": now,
                "schema_version": SCHEMA_VERSION,
            }
