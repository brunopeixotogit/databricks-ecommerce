"""Fact orders: derived from ``purchase`` events grouped by ``order_id``.

Each ``order_id`` may have multiple purchase events (one per line
item). We collapse them into a single order row with an ``items``
struct array, then compute order totals.

Tax and shipping are simulated business rules — in reality these come
from the upstream order-management service.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite


def build_fact_orders(
    spark: SparkSession,
    silver_events: TableRef,
    target: TableRef,
    tax_rate: float = 0.08,
    free_shipping_threshold: float = 75.0,
    flat_shipping: float = 9.99,
) -> None:
    events = spark.read.table(silver_events.fqn)
    purchases = events.filter("event_type = 'purchase' AND order_id IS NOT NULL")

    grouped = (
        purchases
        .groupBy("order_id", "user_id", "session_id_silver", "country", "payment_method")
        .agg(
            F.min("event_ts").alias("order_ts"),
            F.collect_list(
                F.struct(
                    F.col("product_id"),
                    F.col("category"),
                    F.col("quantity"),
                    F.col("price").alias("unit_price"),
                    (F.col("price") * F.col("quantity")).alias("line_total"),
                )
            ).alias("items"),
            F.sum(F.col("price") * F.col("quantity")).alias("subtotal"),
            F.sum("quantity").alias("total_units"),
        )
        .withColumn("status", F.lit("confirmed"))
        .withColumn("currency", F.lit("USD"))
        .withColumn("tax", F.round(F.col("subtotal") * F.lit(tax_rate), 2))
        .withColumn(
            "shipping",
            F.when(F.col("subtotal") > F.lit(free_shipping_threshold), F.lit(0.0))
            .otherwise(F.lit(flat_shipping)),
        )
        .withColumn("total", F.round(F.col("subtotal") + F.col("tax") + F.col("shipping"), 2))
        .withColumn("order_date", F.to_date("order_ts"))
    )

    overwrite(grouped, target, partition_by=["order_date"])
