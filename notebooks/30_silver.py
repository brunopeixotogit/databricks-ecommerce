# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — Silver layer
# MAGIC
# MAGIC Cleansed, deduplicated, sessionized events plus SCD2 dimensions
# MAGIC and an order fact derived from `purchase` events.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",       "main")
dbutils.widgets.text("bronze_schema", "ecom_bronze")
dbutils.widgets.text("silver_schema", "ecom_silver")
dbutils.widgets.text("inactivity_minutes", "30")

CATALOG  = dbutils.widgets.get("catalog")
BRONZE   = dbutils.widgets.get("bronze_schema")
SILVER   = dbutils.widgets.get("silver_schema")
GAP_MIN  = int(dbutils.widgets.get("inactivity_minutes"))

# COMMAND ----------
from src.common.io import TableRef, optimize
from src.silver.events       import build_silver_events
from src.silver.dim_users    import build_dim_users_scd2
from src.silver.dim_products import build_dim_products_scd2
from src.silver.fact_orders  import build_fact_orders

# COMMAND ----------
# MAGIC %md ## silver.events (dedup + sessionize)

# COMMAND ----------
build_silver_events(
    spark,
    bronze=TableRef(CATALOG, BRONZE,  "events_raw"),
    target=TableRef(CATALOG, SILVER, "events"),
    inactivity_minutes=GAP_MIN,
)

# COMMAND ----------
# MAGIC %md ## silver.dim_users_scd2

# COMMAND ----------
build_dim_users_scd2(
    spark,
    bronze=TableRef(CATALOG, BRONZE,  "users_raw"),
    target=TableRef(CATALOG, SILVER, "dim_users_scd2"),
)

# COMMAND ----------
# MAGIC %md ## silver.dim_products_scd2

# COMMAND ----------
build_dim_products_scd2(
    spark,
    bronze=TableRef(CATALOG, BRONZE,  "products_raw"),
    target=TableRef(CATALOG, SILVER, "dim_products_scd2"),
)

# COMMAND ----------
# MAGIC %md ## silver.fact_orders

# COMMAND ----------
build_fact_orders(
    spark,
    silver_events=TableRef(CATALOG, SILVER, "events"),
    target=TableRef(CATALOG, SILVER, "fact_orders"),
)

# COMMAND ----------
# MAGIC %md ## Optimize hot tables (Z-ORDER on common predicates)

# COMMAND ----------
optimize(spark, TableRef(CATALOG, SILVER, "events"),
         zorder=["event_ts", "user_id", "session_id_silver"])
optimize(spark, TableRef(CATALOG, SILVER, "fact_orders"),
         zorder=["order_ts", "user_id"])
