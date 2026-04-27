# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Run the simulator
# MAGIC
# MAGIC Generates a user population, a product catalog, and a stream of
# MAGIC clickstream events into the landing volume. Modes:
# MAGIC
# MAGIC - `bootstrap`  — generate users + products if not yet present, then events.
# MAGIC - `burst`      — generate `n_sessions` events (default; one-shot load).
# MAGIC - `continuous` — keep generating in a loop until the notebook is cancelled.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",     "main")
dbutils.widgets.text("schema",      "ecom_bronze")
dbutils.widgets.text("volume",      "landing")
dbutils.widgets.dropdown("mode",    "burst", ["bootstrap", "burst", "continuous"])
dbutils.widgets.text("n_sessions",  "5000")
dbutils.widgets.text("loop_minutes", "10")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")
MODE    = dbutils.widgets.get("mode")
N_SESS  = int(dbutils.widgets.get("n_sessions"))
LOOP_M  = int(dbutils.widgets.get("loop_minutes"))

# COMMAND ----------
import random, time, uuid
from datetime import datetime, timedelta, timezone

from src.common.config import load_config
from src.simulator.entities import EntityGenerator
from src.simulator.behavior import (
    BehaviorEngine, SessionContext, Product, DEVICES, DEVICE_WEIGHTS,
)
from src.simulator.emit import emit_events, emit_snapshot

cfg = load_config("simulator")
landing      = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
users_dir    = f"{landing}/users"
products_dir = f"{landing}/products"
events_dir   = f"{landing}/events"

# COMMAND ----------
# MAGIC %md ## Bootstrap users + products on first run

# COMMAND ----------
def _has_files(path: str) -> bool:
    try:
        return bool(dbutils.fs.ls(path))
    except Exception:
        return False

gen = EntityGenerator(seed=cfg["seed"])

# We always need an in-memory product list to drive the behavior engine.
products = list(gen.generate_products(
    cfg["products"]["catalog_size"],
    cfg["products"]["category_weights"],
    cfg["products"]["price_ranges"],
    active_rate=cfg["products"].get("active_rate", 0.97),
))
users = list(gen.generate_users(
    cfg["users"]["initial_population"],
    signup_start=datetime.now(timezone.utc) - timedelta(days=365),
    marketing_opt_in_rate=cfg["users"].get("marketing_opt_in_rate", 0.6),
    loyalty_distribution=cfg["users"].get("loyalty_distribution"),
))

if MODE == "bootstrap" or not _has_files(users_dir):
    emit_snapshot(users, users_dir, "users")
    emit_snapshot(products, products_dir, "products")
    print(f"Bootstrapped {len(users)} users, {len(products)} products")

# COMMAND ----------
catalog_objs = [
    Product(product_id=p["product_id"], category=p["category"], price=p["price"])
    for p in products if p["active"]
]

rng = random.Random(cfg["seed"] + 1)
behavior = BehaviorEngine(cfg["behavior"], seed=cfg["seed"] + 2)
anon_rate = float(cfg["traffic"].get("anonymous_session_rate", 0.30))
default_countries = ["US", "GB", "DE", "FR", "BR", "JP", "ES", "IT"]


def session_stream(n_sessions: int):
    now = datetime.now(timezone.utc)
    for i in range(n_sessions):
        is_anon = rng.random() < anon_rate
        u = None if is_anon else rng.choice(users)
        ctx = SessionContext(
            session_id=f"s_{uuid.uuid4().hex[:14]}",
            user_id=None if is_anon else u["user_id"],
            start_ts=now - timedelta(seconds=rng.randint(0, 3600)),
            device=rng.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
            country=(u["country"] if u else rng.choice(default_countries)),
            user_agent="Mozilla/5.0 (sim)",
            ip=f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}",
            referrer=rng.choice([None, "google", "facebook", "instagram", "direct"]),
        )
        yield from behavior.simulate_session(ctx, catalog_objs)

# COMMAND ----------
# MAGIC %md ## Emit events

# COMMAND ----------
if MODE in ("bootstrap", "burst"):
    files = list(emit_events(
        session_stream(N_SESS),
        landing_dir=events_dir,
        rows_per_file=cfg["emission"]["rows_per_file"],
        compress=cfg["emission"].get("compress", True),
    ))
    print(f"Wrote {len(files)} files to {events_dir}")
else:  # continuous
    deadline = time.time() + LOOP_M * 60
    total_files = 0
    while time.time() < deadline:
        batch = max(100, N_SESS // 10)
        files = list(emit_events(
            session_stream(batch),
            landing_dir=events_dir,
            rows_per_file=cfg["emission"]["rows_per_file"],
            compress=cfg["emission"].get("compress", True),
        ))
        total_files += len(files)
        time.sleep(cfg["emission"].get("rotation_seconds", 60))
    print(f"Continuous mode wrote {total_files} files over {LOOP_M} minutes")
