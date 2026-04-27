"""Daily sales mart: GMV / units / AOV by day x category x country.

This is the single source of truth for sales performance dashboards.
Gold tables are deliberately denormalized for query speed.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite


def build_daily_sales(
    spark: SparkSession,
    fact_orders: TableRef,
    target: TableRef,
) -> None:
    orders = spark.read.table(fact_orders.fqn)

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

    daily = (
        line_level
        .groupBy("sale_date", "category", "country")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("line_total"), 2).alias("gmv"),
            F.round(F.avg("total"), 2).alias("avg_order_value"),
        )
    )

    overwrite(daily, target, partition_by=["sale_date"])
