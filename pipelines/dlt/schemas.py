"""Pinned source schemas for the DLT pipeline.

Self-contained copy of the three schemas used by the DLT bronze layer
(``EVENT_SCHEMA``, ``USER_SCHEMA``, ``PRODUCT_SCHEMA``). Keeping this
file inside ``pipelines/dlt/`` avoids the need to manipulate
``sys.path`` or to depend on ``src/`` from a DLT runtime — DLT loads
all files declared as ``libraries`` of the pipeline into one Python
namespace, so a sibling import ``from schemas import ...`` resolves
without any path hacks.

Source of truth for the medallion job remains ``src/common/schemas.py``.
The two definitions are deliberately kept in lockstep — if a producer
schema changes, update both.
"""
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

__all__ = ["EVENT_SCHEMA", "USER_SCHEMA", "PRODUCT_SCHEMA"]

EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),    False),
    StructField("event_type",     StringType(),    False),
    StructField("event_ts",       TimestampType(), False),
    StructField("user_id",        StringType(),    True),
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
