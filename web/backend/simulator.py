"""Server-side session simulator — finite state machine over event types.

Generates synthetic browsing sessions so the pipeline can be exercised
without a human clicking the UI. Mirrors the FSM in
``src/simulator/behavior.py`` but is intentionally minimal and lives
inside the web app process.

State transitions:
    LANDING → BROWSE → (CART → (CHECKOUT → PURCHASE | ABANDON) | EXIT)

Each transition emits one or more events. ``page_view`` is emitted
opportunistically during BROWSE; the cart/checkout/purchase events
carry product / order metadata as required by ``EVENT_SCHEMA``.
"""
from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from .schema import Event

CATEGORIES = {
    "electronics": (49.0, 1499.0),
    "appliances":  (89.0, 2499.0),
    "furniture":   (79.0, 1799.0),
}

# Same product space the frontend catalog uses, so events from the UI
# and from the simulator share product_ids in Bronze.
PRODUCTS = [
    {"product_id": "p_e01", "category": "electronics", "price": 199.0},
    {"product_id": "p_e02", "category": "electronics", "price": 89.0},
    {"product_id": "p_e03", "category": "electronics", "price": 1299.0},
    {"product_id": "p_e04", "category": "electronics", "price": 449.0},
    {"product_id": "p_a01", "category": "appliances",  "price": 299.0},
    {"product_id": "p_a02", "category": "appliances",  "price": 1099.0},
    {"product_id": "p_a03", "category": "appliances",  "price": 159.0},
    {"product_id": "p_a04", "category": "appliances",  "price": 749.0},
    {"product_id": "p_f01", "category": "furniture",   "price": 349.0},
    {"product_id": "p_f02", "category": "furniture",   "price": 89.0},
    {"product_id": "p_f03", "category": "furniture",   "price": 1299.0},
    {"product_id": "p_f04", "category": "furniture",   "price": 459.0},
]

DEVICES = [("mobile", 0.62), ("desktop", 0.30), ("tablet", 0.08)]

PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "boleto"]


def _weighted_choice(rng: random.Random, choices: list[tuple[str, float]]) -> str:
    items, weights = zip(*choices, strict=True)
    return rng.choices(items, weights=weights, k=1)[0]


def _now(rng: random.Random, base: datetime, jitter_max_seconds: int = 30) -> datetime:
    """Advance the synthetic clock by a small random amount."""
    delta = timedelta(seconds=rng.randint(1, jitter_max_seconds))
    return base + delta


def _make_event(
    *,
    event_type: str,
    ts: datetime,
    user_id: str,
    session_id: str,
    device: str,
    country: str,
    page_url: str,
    product: dict | None = None,
    cart_id: str | None = None,
    order_id: str | None = None,
    payment_method: str | None = None,
    quantity: int | None = None,
) -> Event:
    return Event(
        event_id=f"e_{uuid.uuid4().hex}",
        event_type=event_type,           # type: ignore[arg-type]
        event_ts=ts,
        user_id=user_id,
        session_id=session_id,
        device=device,
        user_agent=f"sim/{device}",
        ip=None,
        country=country,
        page_url=page_url,
        referrer=None,
        product_id=product["product_id"] if product else None,
        category=product["category"] if product else None,
        price=product["price"] if product else None,
        quantity=quantity,
        cart_id=cart_id,
        order_id=order_id,
        payment_method=payment_method,
        discount_code=None,
        properties={"source": "server_simulator"},
    )


def simulate_session(
    rng: random.Random,
    *,
    user_id: str | None = None,
    country: str = "US",
    base_ts: datetime | None = None,
) -> Iterator[Event]:
    """Yield a realistic sequence of events for one synthetic session."""
    base_ts = base_ts or datetime.now(timezone.utc)
    user_id = user_id or f"u_{uuid.uuid4().hex[:10]}"
    session_id = f"s_{uuid.uuid4().hex[:12]}"
    device = _weighted_choice(rng, DEVICES)
    cart_id = f"c_{uuid.uuid4().hex[:8]}"

    ts = base_ts

    # LANDING
    ts = _now(rng, ts)
    yield _make_event(
        event_type="page_view",
        ts=ts,
        user_id=user_id,
        session_id=session_id,
        device=device,
        country=country,
        page_url="/",
    )

    # BROWSE — between 2 and 7 product page views
    n_browses = rng.randint(2, 7)
    browsed = rng.sample(PRODUCTS, k=min(n_browses, len(PRODUCTS)))
    for p in browsed:
        ts = _now(rng, ts)
        yield _make_event(
            event_type="page_view",
            ts=ts,
            user_id=user_id,
            session_id=session_id,
            device=device,
            country=country,
            page_url=f"/product/{p['product_id']}",
            product=p,
        )

    # CART decision
    if rng.random() > 0.4:
        return  # exited

    # CART — pick 1–3 of the browsed products
    cart_items = rng.sample(browsed, k=min(rng.randint(1, 3), len(browsed)))
    for p in cart_items:
        qty = rng.randint(1, 3)
        ts = _now(rng, ts)
        yield _make_event(
            event_type="add_to_cart",
            ts=ts,
            user_id=user_id,
            session_id=session_id,
            device=device,
            country=country,
            page_url="/cart",
            product=p,
            cart_id=cart_id,
            quantity=qty,
        )

    # CHECKOUT decision
    if rng.random() > 0.5:
        ts = _now(rng, ts, jitter_max_seconds=120)
        yield _make_event(
            event_type="abandon_cart",
            ts=ts,
            user_id=user_id,
            session_id=session_id,
            device=device,
            country=country,
            page_url="/cart",
            cart_id=cart_id,
        )
        return

    # PURCHASE decision
    if rng.random() > 0.3:
        order_id = f"o_{uuid.uuid4().hex[:12]}"
        payment = rng.choice(PAYMENT_METHODS)
        for p in cart_items:
            qty = rng.randint(1, 3)
            ts = _now(rng, ts)
            yield _make_event(
                event_type="purchase",
                ts=ts,
                user_id=user_id,
                session_id=session_id,
                device=device,
                country=country,
                page_url="/checkout",
                product=p,
                cart_id=cart_id,
                order_id=order_id,
                payment_method=payment,
                quantity=qty,
            )
    else:
        ts = _now(rng, ts, jitter_max_seconds=120)
        yield _make_event(
            event_type="abandon_cart",
            ts=ts,
            user_id=user_id,
            session_id=session_id,
            device=device,
            country=country,
            page_url="/checkout",
            cart_id=cart_id,
        )


def simulate_batch(
    n_sessions: int,
    *,
    seed: int | None = None,
    country: str = "US",
) -> list[Event]:
    """Generate ``n_sessions`` synthetic sessions and flatten the events."""
    rng = random.Random(seed)
    base = datetime.now(timezone.utc)
    out: list[Event] = []
    for _ in range(n_sessions):
        # Each session starts a few seconds after the previous one to keep
        # event_ts roughly monotonic — Silver's session window is per-user
        # so this is cosmetic, not load-bearing.
        base = base + timedelta(seconds=rng.randint(1, 10))
        out.extend(simulate_session(rng, country=country, base_ts=base))
    return out
