"""High-level simulator orchestration.

Bundles the entity bootstrap, session-stream construction, and burst /
continuous emission loops behind a small, testable API. Notebooks call
the functions here so they remain thin orchestration shells; all real
logic lives under ``src.simulator``.

Pure Python — no ``dbutils``, no Spark — so this module is fully
unit-testable on a developer laptop.
"""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator, List, Mapping, Tuple

from src.simulator.behavior import (
    DEVICES,
    DEVICE_WEIGHTS,
    BehaviorEngine,
    Product,
    SessionContext,
)
from src.simulator.emit import emit_events, emit_snapshot
from src.simulator.entities import EntityGenerator

DEFAULT_COUNTRIES: List[str] = ["US", "GB", "DE", "FR", "BR", "JP", "ES", "IT"]
DEFAULT_REFERRERS: List[object] = [None, "google", "facebook", "instagram", "direct"]


@dataclass(frozen=True)
class LandingPaths:
    """Where the simulator writes files inside the landing volume."""

    users: str
    products: str
    events: str

    @classmethod
    def from_root(cls, root: str) -> "LandingPaths":
        return cls(users=f"{root}/users",
                   products=f"{root}/products",
                   events=f"{root}/events")


# ---------------------------------------------------------------------------
# Entity bootstrap
# ---------------------------------------------------------------------------
def bootstrap_entities(
    cfg: Mapping,
    *,
    signup_window_days: int = 365,
    now: datetime | None = None,
) -> Tuple[List[dict], List[dict]]:
    """Generate the deterministic user + product population.

    ``now`` is injectable so tests can pin the clock; production callers
    can omit it.
    """
    now = now or datetime.now(timezone.utc)
    gen = EntityGenerator(seed=cfg["seed"])

    products = list(gen.generate_products(
        cfg["products"]["catalog_size"],
        cfg["products"]["category_weights"],
        cfg["products"]["price_ranges"],
        active_rate=cfg["products"].get("active_rate", 0.97),
    ))
    users = list(gen.generate_users(
        cfg["users"]["initial_population"],
        signup_start=now - timedelta(days=signup_window_days),
        marketing_opt_in_rate=cfg["users"].get("marketing_opt_in_rate", 0.6),
        loyalty_distribution=cfg["users"].get("loyalty_distribution"),
    ))
    return users, products


def write_initial_snapshots(
    users: List[dict],
    products: List[dict],
    paths: LandingPaths,
) -> Tuple[str, str]:
    """Write one users + one products snapshot to the landing volume."""
    u_path = emit_snapshot(users,    paths.users,    "users")
    p_path = emit_snapshot(products, paths.products, "products")
    return u_path, p_path


# ---------------------------------------------------------------------------
# Session stream factory
# ---------------------------------------------------------------------------
SessionStream = Callable[[int], Iterator[dict]]


def make_session_stream(
    users: List[dict],
    products: List[dict],
    cfg: Mapping,
) -> SessionStream:
    """Build a deterministic session-stream generator from entities + cfg.

    Returned callable: ``f(n_sessions) -> Iterator[event_dict]``.
    """
    catalog_objs = [
        Product(product_id=p["product_id"], category=p["category"], price=p["price"])
        for p in products if p["active"]
    ]

    rng = random.Random(cfg["seed"] + 1)
    behavior = BehaviorEngine(cfg["behavior"], seed=cfg["seed"] + 2)
    anon_rate = float(cfg["traffic"].get("anonymous_session_rate", 0.30))

    def _stream(n_sessions: int) -> Iterator[dict]:
        now = datetime.now(timezone.utc)
        for _ in range(n_sessions):
            is_anon = rng.random() < anon_rate
            u = None if is_anon else rng.choice(users)
            ctx = SessionContext(
                session_id=f"s_{uuid.uuid4().hex[:14]}",
                user_id=None if is_anon else u["user_id"],
                start_ts=now - timedelta(seconds=rng.randint(0, 3600)),
                device=rng.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
                country=(u["country"] if u else rng.choice(DEFAULT_COUNTRIES)),
                user_agent="Mozilla/5.0 (sim)",
                ip=f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}",
                referrer=rng.choice(DEFAULT_REFERRERS),
            )
            yield from behavior.simulate_session(ctx, catalog_objs)

    return _stream


# ---------------------------------------------------------------------------
# Emission loops
# ---------------------------------------------------------------------------
def run_burst(
    stream: SessionStream,
    n_sessions: int,
    paths: LandingPaths,
    cfg: Mapping,
) -> List[str]:
    """Emit ``n_sessions`` sessions in a single shot. Returns written paths."""
    return list(emit_events(
        stream(n_sessions),
        landing_dir=paths.events,
        rows_per_file=cfg["emission"]["rows_per_file"],
        compress=cfg["emission"].get("compress", True),
    ))


def run_continuous(
    stream: SessionStream,
    n_sessions_per_minute: int,
    paths: LandingPaths,
    cfg: Mapping,
    loop_minutes: int,
    *,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> int:
    """Loop until ``loop_minutes`` elapses, returning total files written.

    ``sleeper`` and ``clock`` are injectable so tests can fast-forward.
    """
    deadline = clock() + loop_minutes * 60
    total_files = 0
    rotation = cfg["emission"].get("rotation_seconds", 60)
    while clock() < deadline:
        batch = max(100, n_sessions_per_minute // 10)
        files = list(emit_events(
            stream(batch),
            landing_dir=paths.events,
            rows_per_file=cfg["emission"]["rows_per_file"],
            compress=cfg["emission"].get("compress", True),
        ))
        total_files += len(files)
        sleeper(rotation)
    return total_files
