"""DLT silver layer — cleansed, deduped, sessionised, SCD2 dimensions.

Mirrors the transformations in ``src/silver/*.py`` but expressed as DLT
materialised views so the runtime handles target writes, lineage, and
quality reporting. ``dlt.read("events_raw")`` etc. resolve to the
bronze tables defined in ``bronze.py`` within the same pipeline.

Sessionisation and SCD2 are inherently windowed and must run as batch
materialisations; they cannot be incremental streaming tables.
"""
from __future__ import annotations

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

INACTIVITY_MINUTES = int(spark.conf.get("ecom.inactivity_minutes", "30"))  # noqa: F821

VALID_EVENT_TYPES = ("page_view", "add_to_cart", "purchase", "abandon_cart")
USER_TRACKED_COLS = ["email", "country", "city", "marketing_opt_in", "loyalty_tier"]
PRODUCT_TRACKED_COLS = [
    "name",
    "category",
    "subcategory",
    "brand",
    "price",
    "currency",
    "active",
]


@dlt.table(
    name="events",
    comment="Cleansed + deduped + sessionised events. Grain: one row per event_id.",
    partition_cols=["event_date"],
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect_or_fail(
    "valid_event_type",
    "event_type IN ('page_view','add_to_cart','purchase','abandon_cart')",
)
@dlt.expect_or_fail("session_id_silver_present", "session_id_silver IS NOT NULL")
@dlt.expect("non_negative_price", "price IS NULL OR price >= 0")
@dlt.expect("positive_quantity", "quantity IS NULL OR quantity > 0")
@dlt.expect_or_fail(
    "purchase_has_order_id",
    "event_type <> 'purchase' OR order_id IS NOT NULL",
)
def events():
    bronze = dlt.read("events_raw")

    cleaned = (
        bronze.filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isin(*VALID_EVENT_TYPES))
        .filter(F.col("event_ts").isNotNull())
    )

    dedup_window = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
    deduped = (
        cleaned.withColumn("_rk", F.row_number().over(dedup_window))
        .filter("_rk = 1")
        .drop("_rk")
    )

    user_window = Window.partitionBy("user_id").orderBy("event_ts")
    return (
        deduped.withColumn("_prev_ts", F.lag("event_ts").over(user_window))
        .withColumn(
            "_gap_min",
            (F.col("event_ts").cast("long") - F.col("_prev_ts").cast("long")) / 60.0,
        )
        .withColumn(
            "_new_session",
            F.when(
                F.col("_prev_ts").isNull() | (F.col("_gap_min") > INACTIVITY_MINUTES),
                1,
            ).otherwise(0),
        )
        .withColumn("_session_seq", F.sum("_new_session").over(user_window))
        .withColumn(
            "session_id_silver",
            F.concat_ws(
                "_",
                F.coalesce(F.col("user_id"), F.lit("anon")),
                F.col("_session_seq").cast("string"),
            ),
        )
        .withColumnRenamed("session_id", "session_id_raw")
        .drop("_prev_ts", "_gap_min", "_new_session", "_session_seq")
        .withColumn("event_date", F.to_date("event_ts"))
    )


def _scd2(df, key_col: str, tracked_cols: list[str]):
    hashed = df.withColumn(
        "_attr_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256),
    )
    win = Window.partitionBy(key_col).orderBy("updated_ts")
    boundaries = hashed.withColumn("_prev_hash", F.lag("_attr_hash").over(win)).filter(
        F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_attr_hash"))
    )
    return (
        boundaries.withColumn("valid_from", F.col("updated_ts"))
        .withColumn("valid_to", F.lead("updated_ts").over(win))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version", F.row_number().over(win))
        .drop("_prev_hash", "_attr_hash")
    )


@dlt.table(
    name="dim_users_scd2",
    comment="SCD2 user dimension — one row per attribute change.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("user_id_not_null", "user_id IS NOT NULL")
def dim_users_scd2():
    bronze = dlt.read("users_raw").filter(F.col("user_id").isNotNull())
    return _scd2(bronze, "user_id", USER_TRACKED_COLS)


@dlt.table(
    name="dim_products_scd2",
    comment="SCD2 product dimension — tracks price/category/active changes.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("product_id_not_null", "product_id IS NOT NULL")
def dim_products_scd2():
    bronze = dlt.read("products_raw").filter(F.col("product_id").isNotNull())
    return _scd2(bronze, "product_id", PRODUCT_TRACKED_COLS)


@dlt.table(
    name="fact_orders",
    comment="Orders rolled up from purchase events. Grain: one row per order_id.",
    partition_cols=["order_date"],
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("order_total_positive", "total >= 0")
@dlt.expect_or_fail("order_subtotal_consistent", "subtotal >= 0")
@dlt.expect_or_fail("order_has_items", "size(items) > 0")
@dlt.expect(
    "order_total_matches",
    "abs(total - (subtotal + tax + shipping)) < 0.05",
)
def fact_orders():
    events = dlt.read("events")
    purchases = events.filter("event_type = 'purchase' AND order_id IS NOT NULL")

    tax_rate = 0.08
    free_shipping_threshold = 75.0
    flat_shipping = 9.99

    return (
        purchases.groupBy(
            "order_id", "user_id", "session_id_silver", "country", "payment_method"
        )
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
        .withColumn(
            "total", F.round(F.col("subtotal") + F.col("tax") + F.col("shipping"), 2)
        )
        .withColumn("order_date", F.to_date("order_ts"))
    )
