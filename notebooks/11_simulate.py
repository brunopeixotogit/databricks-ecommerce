# Databricks notebook source
# MAGIC %md
# MAGIC #  11 — Synthetic event simulator (decoupled)
# MAGIC
# MAGIC Generates exactly `n_events` synthetic events and writes them as one
# MAGIC NDJSON file under
# MAGIC `/Volumes/<catalog>/<bronze_schema>/<volume>/events/dt=YYYY-MM-DD/`.
# MAGIC
# MAGIC This notebook is **independent of the processing pipeline**: it does
# MAGIC not call DLT, does not run Bronze/Silver/Gold, does not touch any
# MAGIC Delta table. Run it whenever you need fresh test data; trigger DLT
# MAGIC separately when you want to process it.
# MAGIC
# MAGIC Used by:
# MAGIC - Asset bundle job `ecom_orchestrator` (modes `simulator` / `full`)
# MAGIC - Manual ad-hoc runs from the workspace UI
# MAGIC
# MAGIC The legacy `notebooks/10_run_simulator.py` is preserved for the
# MAGIC `medallion` job — that job still bootstraps users/products snapshots
# MAGIC alongside events. This notebook only writes events.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Install runtime dependencies

# COMMAND ----------
# MAGIC %pip install --quiet "faker>=24.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",  "dev_main")
dbutils.widgets.text("schema",   "ecom_bronze")
dbutils.widgets.text("volume",   "landing")
dbutils.widgets.text("n_events", "5000")
dbutils.widgets.text("dt",       "")     # empty = today (UTC)
dbutils.widgets.text("source",   "simulator")
# `mode` is forwarded by the orchestrator job. When the orchestrator
# is invoked with mode=prod, the simulator must not produce any new
# events. We achieve this by short-circuiting at the top of this
# notebook rather than via a Databricks `condition_task` exclusion,
# because the latter cannot coexist with a `simulator-before-dlt`
# ordering dependency on `dlt_pipeline` (Databricks Jobs ignores the
# `outcome` qualifier under `run_if: ALL_DONE` / `NONE_FAILED`).
dbutils.widgets.text("mode",     "simulator")

CATALOG  = dbutils.widgets.get("catalog")
SCHEMA   = dbutils.widgets.get("schema")
VOLUME   = dbutils.widgets.get("volume")
N_EVENTS = int(dbutils.widgets.get("n_events"))
DT       = dbutils.widgets.get("dt") or None
SOURCE   = dbutils.widgets.get("source")
MODE     = dbutils.widgets.get("mode")

# Production-safety short-circuit. mode=prod means data processing only;
# this notebook must not generate synthetic events.
if MODE == "prod":
    print(f"mode={MODE!r} -> simulator is a no-op in production mode.")
    dbutils.notebook.exit("skipped: mode=prod")

# COMMAND ----------
from src.simulator.api import generate_events

result = generate_events(
    n_events=N_EVENTS,
    date=DT,
    landing_root=f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}",
    source=SOURCE,
)

print(f"Wrote {result['events_written']} events")
print(f"Partition : {result['partition']}")
print(f"Path      : {result['path']}")
print(f"Source    : {result['source']}")
print(f"Run id    : {result['run_id']}")

# Surface the run summary so the orchestrator can log it.
dbutils.notebook.exit(str(result))
