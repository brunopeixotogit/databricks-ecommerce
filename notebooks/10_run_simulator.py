# Databricks notebook source
# MAGIC %md
# MAGIC #  10 — Run the simulator
# MAGIC
# MAGIC Generates a user population, a product catalog, and a stream of
# MAGIC clickstream events into the landing volume. Modes:
# MAGIC
# MAGIC - `bootstrap`  — generate users + products if not yet present, then events.
# MAGIC - `burst`      — generate `n_sessions` events (default; one-shot load).
# MAGIC - `continuous` — keep generating in a loop until the notebook is cancelled.
# MAGIC
# MAGIC This notebook is a **thin orchestration shell**: it loads config,
# MAGIC reads widgets, decides bootstrap vs. resume, and delegates everything
# MAGIC else to `src.simulator.run`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Install runtime dependencies
# MAGIC
# MAGIC `faker` is not bundled with the Databricks Free Edition runtime, so we
# MAGIC install it on every run before importing the simulator. The
# MAGIC `restartPython()` below is required for the new package to be picked up.

# COMMAND ----------
# MAGIC %pip install --quiet "faker>=24.0"

# COMMAND ----------
dbutils.library.restartPython()

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
from src.common.config import load_config
from src.simulator.run import (
    LandingPaths,
    bootstrap_entities,
    make_session_stream,
    run_burst,
    run_continuous,
    write_initial_snapshots,
)

cfg   = load_config("simulator")
paths = LandingPaths.from_root(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")

# COMMAND ----------
# MAGIC %md ## Bootstrap users + products on first run

# COMMAND ----------
def _has_files(path: str) -> bool:
    """Databricks-only existence check; left in the notebook because it
    relies on `dbutils.fs` which has no local equivalent."""
    try:
        return bool(dbutils.fs.ls(path))
    except Exception:
        return False


users, products = bootstrap_entities(cfg)

if MODE == "bootstrap" or not _has_files(paths.users):
    u_path, p_path = write_initial_snapshots(users, products, paths)
    print(f"Bootstrapped {len(users)} users → {u_path}")
    print(f"Bootstrapped {len(products)} products → {p_path}")

# COMMAND ----------
# MAGIC %md ## Emit events

# COMMAND ----------
session_stream = make_session_stream(users, products, cfg)

if MODE in ("bootstrap", "burst"):
    files = run_burst(session_stream, N_SESS, paths, cfg)
    print(f"Wrote {len(files)} files to {paths.events}")
else:
    total = run_continuous(session_stream, N_SESS, paths, cfg, LOOP_M)
    print(f"Continuous mode wrote {total} files over {LOOP_M} minutes")
