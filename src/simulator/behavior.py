"""Session-level behavioral state machine.

States and transitions::

    LANDING ──► BROWSE (1..N page_views)
                  ├──► EXIT
                  └──► CART (1..K add_to_cart)
                         ├──► ABANDON_CART (idle timeout)
                         └──► CHECKOUT
                                ├──► ABANDON_CART (payment failure)
                                └──► PURCHASE (1 order, K line items)

Probabilities and rate parameters come from ``conf/simulator.yml`` so
the model is tunable without code changes. The engine yields fully
formed event dicts that conform to ``EVENT_SCHEMA``.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterator, List, Mapping, Optional

from src.common.version import SCHEMA_VERSION

DEVICES: List[str] = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS: List[float] = [0.62, 0.30, 0.08]

PAYMENT_METHODS: List[str] = [
    "credit_card", "paypal", "apple_pay", "google_pay", "klarna"
]


@dataclass(frozen=True)
class Product:
    """Lightweight catalog row consumed by the behavior engine."""

    product_id: str
    category: str
    price: float


@dataclass
class SessionContext:
    session_id: str
    user_id: Optional[str]
    start_ts: datetime
    device: str
    country: str
    user_agent: str
    ip: str
    referrer: Optional[str]
    cart_id: Optional[str] = None
    cart_items: List[dict] = field(default_factory=list)


class BehaviorEngine:
    """Generates the event sequence for one session."""

    def __init__(self, behavior_cfg: Mapping[str, float], seed: int = 0):
        self.cfg = behavior_cfg
        self.rng = random.Random(seed)

    def simulate_session(
        self,
        ctx: SessionContext,
        catalog: List[Product],
    ) -> Iterator[dict]:
        if not catalog:
            return

        # 1) Page views — exponential dwell, capped to keep tails reasonable.
        lam = float(self.cfg["page_views_per_session_lambda"])
        n_views = max(1, min(30, int(self.rng.expovariate(1.0 / lam))))

        ts = ctx.start_ts
        viewed: List[Product] = []
        for _ in range(n_views):
            ts += timedelta(seconds=self.rng.randint(5, 90))
            product = self.rng.choice(catalog)
            viewed.append(product)
            yield self._event("page_view", ctx, ts, product=product)

        # 2) Add to cart?
        if self.rng.random() >= float(self.cfg["add_to_cart_probability"]):
            return

        ctx.cart_id = f"c_{uuid.uuid4().hex[:12]}"
        n_items = self.rng.randint(1, min(4, len(viewed)))
        chosen = self.rng.sample(viewed, k=n_items)
        for product in chosen:
            ts += timedelta(seconds=self.rng.randint(3, 40))
            qty = self.rng.choices([1, 2, 3], weights=[0.80, 0.15, 0.05])[0]
            ctx.cart_items.append({"product": product, "quantity": qty})
            yield self._event("add_to_cart", ctx, ts, product=product, quantity=qty)

        # 3) Checkout?
        if self.rng.random() >= float(self.cfg["checkout_given_cart"]):
            ts += timedelta(minutes=int(self.cfg["abandonment_idle_minutes"]))
            yield self._event("abandon_cart", ctx, ts)
            return

        ts += timedelta(seconds=self.rng.randint(20, 180))
        payment_method = self.rng.choice(PAYMENT_METHODS)

        # 4) Purchase?
        if self.rng.random() >= float(self.cfg["purchase_given_checkout"]):
            yield self._event(
                "abandon_cart", ctx, ts, payment_method=payment_method
            )
            return

        order_id = f"o_{uuid.uuid4().hex[:12]}"
        ts += timedelta(seconds=self.rng.randint(2, 15))
        for item in ctx.cart_items:
            yield self._event(
                "purchase",
                ctx,
                ts,
                product=item["product"],
                quantity=item["quantity"],
                order_id=order_id,
                payment_method=payment_method,
            )

    # ------------------------------------------------------------------
    def _event(
        self,
        event_type: str,
        ctx: SessionContext,
        ts: datetime,
        product: Optional[Product] = None,
        quantity: Optional[int] = None,
        order_id: Optional[str] = None,
        payment_method: Optional[str] = None,
    ) -> dict:
        return {
            "event_id": uuid.uuid4().hex,
            "event_type": event_type,
            "event_ts": ts,
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
            "device": ctx.device,
            "user_agent": ctx.user_agent,
            "ip": ctx.ip,
            "country": ctx.country,
            "page_url": (
                f"/{product.category}/{product.product_id}" if product else "/"
            ),
            "referrer": ctx.referrer,
            "product_id": product.product_id if product else None,
            "category": product.category if product else None,
            "price": product.price if product else None,
            "quantity": quantity,
            "cart_id": ctx.cart_id,
            "order_id": order_id,
            "payment_method": payment_method,
            "discount_code": None,
            "properties": {},
            "schema_version": SCHEMA_VERSION,
        }


# ---------------------------------------------------------------------------
def diurnal_factor(hour: int) -> float:
    """Crude 24-hour traffic curve, peaking around 20:00 local time.

    Returns a multiplier in roughly [0.4, 1.0]. Combine with a base
    sessions-per-minute rate to drive realistic time-of-day load.
    """
    return 0.4 + 0.6 * math.exp(-((hour - 20) ** 2) / 24.0)
