# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — Bronze ingestion
# MAGIC
# MAGIC Auto Loader streams from the landing volume into the Bronze
# MAGIC Delta tables. Three streams: events, users, products.
# MAGIC
# MAGIC Trigger defaults to `availableNow` so this notebook processes
# MAGIC any new files and exits — ideal for Workflow scheduling and
# MAGIC for keeping cluster cost in check on Free Edition.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",       "main")
dbutils.widgets.text("bronze_schema", "ecom_bronze")
dbutils.widgets.text("volume",        "landing")
dbutils.widgets.dropdown("trigger",   "availableNow", ["availableNow", "continuous"])

CATALOG = dbutils.widgets.get("catalog")
BRONZE  = dbutils.widgets.get("bronze_schema")
VOLUME  = dbutils.widgets.get("volume")
TRIGGER = dbutils.widgets.get("trigger")

# COMMAND ----------
from src.common.io import TableRef
from src.common.schemas import EVENT_SCHEMA, USER_SCHEMA, PRODUCT_SCHEMA
from src.bronze.ingest_events import stream_events, stream_snapshot

base = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"

# COMMAND ----------
# MAGIC %md ## Events stream

# COMMAND ----------
stream_events(
    spark,
    landing_path=f"{base}/events",
    checkpoint_path=f"{base}/_checkpoints/events",
    target=TableRef(CATALOG, BRONZE, "events_raw"),
    schema=EVENT_SCHEMA,
    trigger=TRIGGER,
)

# COMMAND ----------
# MAGIC %md ## Users snapshot stream

# COMMAND ----------
stream_snapshot(
    spark,
    landing_path=f"{base}/users",
    checkpoint_path=f"{base}/_checkpoints/users",
    schema=USER_SCHEMA,
    target=TableRef(CATALOG, BRONZE, "users_raw"),
    trigger=TRIGGER,
)

# COMMAND ----------
# MAGIC %md ## Products snapshot stream

# COMMAND ----------
stream_snapshot(
    spark,
    landing_path=f"{base}/products",
    checkpoint_path=f"{base}/_checkpoints/products",
    schema=PRODUCT_SCHEMA,
    target=TableRef(CATALOG, BRONZE, "products_raw"),
    trigger=TRIGGER,
)

# COMMAND ----------
# MAGIC %md ## Sanity check

# COMMAND ----------
display(spark.sql(f"""
    SELECT 'events_raw'   AS table, COUNT(*) AS rows FROM {CATALOG}.{BRONZE}.events_raw
    UNION ALL
    SELECT 'users_raw',           COUNT(*)         FROM {CATALOG}.{BRONZE}.users_raw
    UNION ALL
    SELECT 'products_raw',        COUNT(*)         FROM {CATALOG}.{BRONZE}.products_raw
"""))
