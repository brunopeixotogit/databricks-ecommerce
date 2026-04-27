"""dim_user_360: per-user lifetime metrics for marketing / CRM.

Combines the current-version user dimension with order facts and event
activity to produce a per-user RFM-style summary plus a coarse
lifecycle segment. This table powers segmentation in the marketing
tool and is the natural starting point for ML feature engineering.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite


def build_user_360(
    spark: SparkSession,
    dim_users: TableRef,
    fact_orders: TableRef,
    silver_events: TableRef,
    target: TableRef,
) -> None:
    users = spark.read.table(dim_users.fqn).filter("is_current = true")
    orders = spark.read.table(fact_orders.fqn)
    events = spark.read.table(silver_events.fqn)

    order_metrics = (
        orders
        .groupBy("user_id")
        .agg(
            F.count("order_id").alias("orders_count"),
            F.round(F.sum("total"), 2).alias("ltv"),
            F.round(F.avg("total"), 2).alias("avg_order_value"),
            F.max("order_ts").alias("last_order_ts"),
            F.min("order_ts").alias("first_order_ts"),
        )
    )

    activity = (
        events
        .groupBy("user_id")
        .agg(
            F.countDistinct("session_id_silver").alias("sessions_count"),
            F.max("event_ts").alias("last_seen_ts"),
        )
    )

    out = (
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

    overwrite(out, target)
