# Databricks notebook source
# MAGIC %md
# MAGIC # 40 — Gold layer
# MAGIC
# MAGIC Consumer-ready marts: daily sales, conversion funnel, abandoned
# MAGIC carts, and a per-user 360 table.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",       "main")
dbutils.widgets.text("silver_schema", "ecom_silver")
dbutils.widgets.text("gold_schema",   "ecom_gold")

CATALOG = dbutils.widgets.get("catalog")
SILVER  = dbutils.widgets.get("silver_schema")
GOLD    = dbutils.widgets.get("gold_schema")

# COMMAND ----------
from src.common.io import TableRef, optimize
from src.gold.daily_sales      import build_daily_sales
from src.gold.funnel           import build_funnel
from src.gold.abandoned_carts  import build_abandoned_carts
from src.gold.user_360         import build_user_360

# COMMAND ----------
build_daily_sales(
    spark,
    fact_orders=TableRef(CATALOG, SILVER, "fact_orders"),
    target=TableRef(CATALOG, GOLD,   "fact_daily_sales"),
)

# COMMAND ----------
build_funnel(
    spark,
    silver_events=TableRef(CATALOG, SILVER, "events"),
    target=TableRef(CATALOG, GOLD,   "fact_funnel"),
)

# COMMAND ----------
build_abandoned_carts(
    spark,
    silver_events=TableRef(CATALOG, SILVER, "events"),
    target=TableRef(CATALOG, GOLD,   "fact_abandoned_carts"),
)

# COMMAND ----------
build_user_360(
    spark,
    dim_users=TableRef(CATALOG, SILVER, "dim_users_scd2"),
    fact_orders=TableRef(CATALOG, SILVER, "fact_orders"),
    silver_events=TableRef(CATALOG, SILVER, "events"),
    target=TableRef(CATALOG, GOLD,   "dim_user_360"),
)

# COMMAND ----------
# MAGIC %md ## Z-ORDER hot Gold tables

# COMMAND ----------
optimize(spark, TableRef(CATALOG, GOLD, "fact_daily_sales"),
         zorder=["sale_date", "category"])

# COMMAND ----------
# MAGIC %md ## Quick smoke

# COMMAND ----------
display(spark.sql(f"""
    SELECT sale_date, category, country, orders, units, gmv, avg_order_value
    FROM   {CATALOG}.{GOLD}.fact_daily_sales
    ORDER  BY sale_date DESC, gmv DESC
    LIMIT  50
"""))
