"""DLT gold layer — denormalised marts for BI / CRM / ML feature inputs.

Mirrors the transformations in ``src/gold/*.py`` but as DLT materialised
views. Each table is rebuilt from silver on every pipeline run; full
rebuild keeps the marts cheap and brutally simple to reason about.
"""
from __future__ import annotations

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="fact_daily_sales",
    comment="Daily GMV / units / AOV by category × country.",
    partition_cols=["sale_date"],
    table_properties={"quality": "gold"},
)
@dlt.expect("non_negative_gmv", "gmv >= 0")
@dlt.expect("orders_match_units", "units >= orders")
def fact_daily_sales():
    orders = dlt.read("fact_orders")

    line_level = orders.select(
        F.to_date("order_ts").alias("sale_date"),
        F.col("country"),
        F.col("order_id"),
        F.col("total"),
        F.explode("items").alias("item"),
    ).select(
        "sale_date",
        "country",
        "order_id",
        "total",
        F.col("item.category").alias("category"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.line_total").alias("line_total"),
    )

    return line_level.groupBy("sale_date", "category", "country").agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("line_total"), 2).alias("gmv"),
        F.round(F.avg("total"), 2).alias("avg_order_value"),
    )


@dlt.table(
    name="fact_funnel",
    comment="Daily session-level conversion funnel: view → cart → purchase.",
    partition_cols=["event_date"],
    table_properties={"quality": "gold"},
)
@dlt.expect("rates_in_unit_interval",
            "(cart_rate IS NULL OR (cart_rate BETWEEN 0 AND 1)) AND "
            "(purchase_rate IS NULL OR (purchase_rate BETWEEN 0 AND 1)) AND "
            "(abandon_rate IS NULL OR (abandon_rate BETWEEN 0 AND 1))")
def fact_funnel():
    events = dlt.read("events")

    sessions = events.groupBy(
        F.to_date("event_ts").alias("event_date"),
        F.col("session_id_silver"),
    ).agg(
        F.max((F.col("event_type") == "page_view").cast("int")).alias("viewed"),
        F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("carted"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased"),
        F.max((F.col("event_type") == "abandon_cart").cast("int")).alias("abandoned"),
    )

    return (
        sessions.groupBy("event_date")
        .agg(
            F.sum("viewed").alias("sessions_viewed"),
            F.sum("carted").alias("sessions_carted"),
            F.sum("purchased").alias("sessions_purchased"),
            F.sum("abandoned").alias("sessions_abandoned"),
        )
        .withColumn(
            "cart_rate",
            F.when(F.col("sessions_viewed") > 0,
                   F.col("sessions_carted") / F.col("sessions_viewed")),
        )
        .withColumn(
            "purchase_rate",
            F.when(F.col("sessions_viewed") > 0,
                   F.col("sessions_purchased") / F.col("sessions_viewed")),
        )
        .withColumn(
            "abandon_rate",
            F.when(F.col("sessions_carted") > 0,
                   F.col("sessions_abandoned") / F.col("sessions_carted")),
        )
    )


@dlt.table(
    name="fact_abandoned_carts",
    comment="Sessions with cart events but no purchase — remarketing candidates.",
    partition_cols=["cart_date"],
    table_properties={"quality": "gold"},
)
@dlt.expect("non_negative_cart_value", "cart_value >= 0")
def fact_abandoned_carts():
    events = dlt.read("events")
    cart_events = events.filter("event_type = 'add_to_cart'")

    cart_value = cart_events.groupBy(
        "session_id_silver", "user_id", "cart_id", "country"
    ).agg(
        F.min("event_ts").alias("cart_started_ts"),
        F.max("event_ts").alias("last_cart_event_ts"),
        F.round(F.sum(F.col("price") * F.col("quantity")), 2).alias("cart_value"),
        F.sum("quantity").alias("items_in_cart"),
        F.collect_set("category").alias("categories"),
    )

    purchases = (
        events.filter("event_type = 'purchase'")
        .select("session_id_silver")
        .distinct()
    )

    return (
        cart_value.join(purchases, "session_id_silver", "left_anti")
        .withColumn("cart_date", F.to_date("cart_started_ts"))
        .withColumn("remarketable", F.col("user_id").isNotNull())
    )


@dlt.table(
    name="dim_user_360",
    comment="Per-user lifetime metrics for marketing / CRM / ML feature engineering.",
    table_properties={"quality": "gold"},
)
@dlt.expect("known_segment",
            "segment IN ('prospect','active','lapsing','churned')")
def dim_user_360():
    users = dlt.read("dim_users_scd2").filter("is_current = true")
    orders = dlt.read("fact_orders")
    events = dlt.read("events")

    order_metrics = orders.groupBy("user_id").agg(
        F.count("order_id").alias("orders_count"),
        F.round(F.sum("total"), 2).alias("ltv"),
        F.round(F.avg("total"), 2).alias("avg_order_value"),
        F.max("order_ts").alias("last_order_ts"),
        F.min("order_ts").alias("first_order_ts"),
    )

    activity = events.groupBy("user_id").agg(
        F.countDistinct("session_id_silver").alias("sessions_count"),
        F.max("event_ts").alias("last_seen_ts"),
    )

    return (
        users.alias("u")
        .join(order_metrics, "user_id", "left")
        .join(activity, "user_id", "left")
        .withColumn(
            "recency_days",
            F.when(
                F.col("last_order_ts").isNotNull(),
                F.datediff(F.current_date(), F.to_date("last_order_ts")),
            ),
        )
        .withColumn(
            "segment",
            F.when(F.col("orders_count").isNull(), F.lit("prospect"))
            .when(F.col("recency_days") <= 30, F.lit("active"))
            .when(F.col("recency_days") <= 180, F.lit("lapsing"))
            .otherwise(F.lit("churned")),
        )
    )
