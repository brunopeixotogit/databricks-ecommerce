# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Quality checks
# MAGIC
# MAGIC Lightweight expectations on the Silver and Gold layers. Run as
# MAGIC the final task of the Workflow; failures abort the Workflow run.

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
from src.common.quality import Expectation, evaluate, enforce

# COMMAND ----------
events = spark.read.table(f"{CATALOG}.{SILVER}.events")

event_expectations = [
    Expectation("event_id_not_null",
                "event_id IS NOT NULL", "fail"),
    Expectation("valid_event_type",
                "event_type IN ('page_view','add_to_cart','purchase','abandon_cart')", "fail"),
    Expectation("session_id_silver_present",
                "session_id_silver IS NOT NULL", "fail"),
    Expectation("non_negative_price",
                "price IS NULL OR price >= 0", "warn"),
    Expectation("positive_quantity",
                "quantity IS NULL OR quantity > 0", "warn"),
    Expectation("purchase_has_order_id",
                "event_type <> 'purchase' OR order_id IS NOT NULL", "fail"),
]
event_results = evaluate(events, event_expectations)
for r in event_results:
    print(r)

# COMMAND ----------
orders = spark.read.table(f"{CATALOG}.{SILVER}.fact_orders")

order_expectations = [
    Expectation("order_total_positive",     "total >= 0",          "fail"),
    Expectation("order_subtotal_consistent","subtotal >= 0",       "fail"),
    Expectation("order_has_items",          "size(items) > 0",     "fail"),
    Expectation("order_total_matches",
                "abs(total - (subtotal + tax + shipping)) < 0.05", "warn"),
]
order_results = evaluate(orders, order_expectations)
for r in order_results:
    print(r)

# COMMAND ----------
# MAGIC %md ## Enforce: fail the run if any `fail` expectation has failures

# COMMAND ----------
enforce(event_results)
enforce(order_results)
print("All quality gates passed.")
