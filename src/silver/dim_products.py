"""SCD Type 2 dimension for the product catalog.

Tracks price, category, branding, and active-flag changes. A retailer
typically wants to attribute past orders to the price *at the time of
sale*, which is exactly what SCD2 enables: join on ``product_id`` and
``order_ts BETWEEN valid_from AND valid_to``.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite

TRACKED_COLS = ["name", "category", "subcategory", "brand", "price", "currency", "active"]


def build_dim_products_scd2(
    spark: SparkSession,
    bronze: TableRef,
    target: TableRef,
) -> None:
    bronze_df = spark.read.table(bronze.fqn).filter(F.col("product_id").isNotNull())

    hashed = bronze_df.withColumn(
        "_attr_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in TRACKED_COLS]), 256),
    )

    win = Window.partitionBy("product_id").orderBy("updated_ts")
    boundaries = (
        hashed
        .withColumn("_prev_hash", F.lag("_attr_hash").over(win))
        .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_attr_hash")))
    )

    versions = (
        boundaries
        .withColumn("valid_from", F.col("updated_ts"))
        .withColumn("valid_to", F.lead("updated_ts").over(win))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version", F.row_number().over(win))
        .drop("_prev_hash", "_attr_hash")
    )

    overwrite(versions, target)
