"""Abandoned-cart mart: candidates for remarketing campaigns.

A cart is *abandoned* when the session contained ``add_to_cart`` events
but no ``purchase``. Cart value at abandonment is summed from the
add_to_cart prices (these reflect the user's intent, not the final
order). Only sessions with a known ``user_id`` (and email) are useful
for email/SMS remarketing — anonymous abandons go to retargeting ads.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite


def build_abandoned_carts(
    spark: SparkSession,
    silver_events: TableRef,
    target: TableRef,
) -> None:
    events = spark.read.table(silver_events.fqn)

    cart_events = events.filter("event_type = 'add_to_cart'")
    cart_value = (
        cart_events
        .groupBy("session_id_silver", "user_id", "cart_id", "country")
        .agg(
            F.min("event_ts").alias("cart_started_ts"),
            F.max("event_ts").alias("last_cart_event_ts"),
            F.round(F.sum(F.col("price") * F.col("quantity")), 2).alias("cart_value"),
            F.sum("quantity").alias("items_in_cart"),
            F.collect_set("category").alias("categories"),
        )
    )

    purchases = (
        events.filter("event_type = 'purchase'")
        .select("session_id_silver")
        .distinct()
    )

    abandoned = (
        cart_value
        .join(purchases, "session_id_silver", "left_anti")
        .withColumn("cart_date", F.to_date("cart_started_ts"))
        .withColumn(
            "remarketable",
            F.col("user_id").isNotNull(),
        )
    )

    overwrite(abandoned, target, partition_by=["cart_date"])
