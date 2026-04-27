# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Platform setup
# MAGIC
# MAGIC Idempotently creates the Bronze / Silver / Gold schemas and the
# MAGIC `landing` Volume used by the simulator and Auto Loader.
# MAGIC
# MAGIC Free Edition ships with the `main` catalog already provisioned,
# MAGIC so we do not attempt to create a catalog here.

# COMMAND ----------
import os, sys
# Make `src.` importable when running from a Databricks Repo / Git folder.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",       "main")
dbutils.widgets.text("bronze_schema", "ecom_bronze")
dbutils.widgets.text("silver_schema", "ecom_silver")
dbutils.widgets.text("gold_schema",   "ecom_gold")
dbutils.widgets.text("volume_name",   "landing")

CATALOG = dbutils.widgets.get("catalog")
BRONZE  = dbutils.widgets.get("bronze_schema")
SILVER  = dbutils.widgets.get("silver_schema")
GOLD    = dbutils.widgets.get("gold_schema")
VOLUME  = dbutils.widgets.get("volume_name")

# COMMAND ----------
# MAGIC %md ## Schemas + Volume

# COMMAND ----------
for s in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{s}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{BRONZE}`.`{VOLUME}`")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Landing-volume layout
# MAGIC
# MAGIC Pre-create the subfolders so the simulator and Auto Loader use
# MAGIC the same paths.

# COMMAND ----------
base = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
for sub in ("events", "users", "products", "orders", "_checkpoints"):
    path = f"{base}/{sub}"
    try:
        dbutils.fs.mkdirs(path)
    except Exception as e:
        print(f"mkdirs({path}) -> {e}")

print(f"Setup complete. Landing volume: {base}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify
# MAGIC List schemas and the landing root so the user can confirm.

# COMMAND ----------
display(spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`"))

# COMMAND ----------
display(dbutils.fs.ls(base))
