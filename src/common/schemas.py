"""Pinned schemas for every source.

Never rely on ``inferSchema`` in production: a single bad row from an
upstream producer can silently change a column type and corrupt every
downstream join. Changing a schema here is a breaking change — bump
``SCHEMA_VERSION`` and coordinate with the producer.
"""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.version import SCHEMA_VERSION  # re-exported for back-compat

__all__ = [
    "SCHEMA_VERSION",
    "EVENT_SCHEMA",
    "USER_SCHEMA",
    "PRODUCT_SCHEMA",
    "ORDER_ITEM_SCHEMA",
    "ORDER_SCHEMA",
]

# ---------------------------------------------------------------------------
# Event envelope: shared by page_view, add_to_cart, purchase, abandon_cart.
# Optional fields are typed with nullable=True; the event_type discriminator
# tells downstream code which fields to expect.
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),    False),
    StructField("event_type",     StringType(),    False),
    StructField("event_ts",       TimestampType(), False),
    StructField("user_id",        StringType(),    True),   # null for anonymous
    StructField("session_id",     StringType(),    False),
    StructField("device",         StringType(),    True),
    StructField("user_agent",     StringType(),    True),
    StructField("ip",             StringType(),    True),
    StructField("country",        StringType(),    True),
    StructField("page_url",       StringType(),    True),
    StructField("referrer",       StringType(),    True),
    StructField("product_id",     StringType(),    True),
    StructField("category",       StringType(),    True),
    StructField("price",          DoubleType(),    True),
    StructField("quantity",       IntegerType(),   True),
    StructField("cart_id",        StringType(),    True),
    StructField("order_id",       StringType(),    True),
    StructField("payment_method", StringType(),    True),
    StructField("discount_code",  StringType(),    True),
    StructField("properties",     MapType(StringType(), StringType()), True),
    StructField("schema_version", StringType(),    False),
])

USER_SCHEMA = StructType([
    StructField("user_id",          StringType(),    False),
    StructField("email",            StringType(),    True),
    StructField("first_name",       StringType(),    True),
    StructField("last_name",        StringType(),    True),
    StructField("country",          StringType(),    True),
    StructField("city",             StringType(),    True),
    StructField("signup_ts",        TimestampType(), False),
    StructField("marketing_opt_in", BooleanType(),   True),
    StructField("loyalty_tier",     StringType(),    True),
    StructField("updated_ts",       TimestampType(), False),
    StructField("schema_version",   StringType(),    False),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id",     StringType(),    False),
    StructField("sku",            StringType(),    False),
    StructField("name",           StringType(),    False),
    StructField("category",       StringType(),    False),
    StructField("subcategory",    StringType(),    True),
    StructField("brand",          StringType(),    True),
    StructField("price",          DoubleType(),    False),
    StructField("currency",       StringType(),    False),
    StructField("active",         BooleanType(),   False),
    StructField("updated_ts",     TimestampType(), False),
    StructField("schema_version", StringType(),    False),
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(),  False),
    StructField("sku",        StringType(),  True),
    StructField("category",   StringType(),  True),
    StructField("quantity",   IntegerType(), False),
    StructField("unit_price", DoubleType(),  False),
])

ORDER_SCHEMA = StructType([
    StructField("order_id",       StringType(),    False),
    StructField("user_id",        StringType(),    False),
    StructField("session_id",     StringType(),    True),
    StructField("order_ts",       TimestampType(), False),
    StructField("status",         StringType(),    False),
    StructField("currency",       StringType(),    False),
    StructField("subtotal",       DoubleType(),    False),
    StructField("tax",            DoubleType(),    False),
    StructField("shipping",       DoubleType(),    False),
    StructField("total",          DoubleType(),    False),
    StructField("payment_method", StringType(),    True),
    StructField("country",        StringType(),    True),
    StructField("items",          ArrayType(ORDER_ITEM_SCHEMA), False),
    StructField("schema_version", StringType(),    False),
])
