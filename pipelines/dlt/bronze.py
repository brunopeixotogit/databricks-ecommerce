"""DLT bronze layer — streaming tables backed by Auto Loader.

Mirrors ``notebooks/20_bronze.py`` / ``src/bronze/ingest_events.py`` but
expressed declaratively with ``@dlt.table`` so the Delta Live Tables
runtime owns checkpointing, expectations, and lineage. Source-faithful
capture only — business rules belong in silver.

Pipeline configuration is read from ``spark.conf`` keys prefixed with
``ecom.``; these are populated by the ``configuration`` block on the
DLT pipeline resource in ``databricks.yml``.
"""
from __future__ import annotations

import dlt
from pyspark.sql import functions as F

from src.common.schemas import EVENT_SCHEMA, PRODUCT_SCHEMA, USER_SCHEMA

CATALOG = spark.conf.get("ecom.catalog")  # noqa: F821 - spark provided by DLT runtime
BRONZE_SCHEMA = spark.conf.get("ecom.bronze_schema")  # noqa: F821
VOLUME = spark.conf.get("ecom.volume", "landing")  # noqa: F821

LANDING_BASE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}"

_CLOUD_FILES_OPTIONS = {
    "cloudFiles.format": "json",
    "cloudFiles.inferColumnTypes": "false",
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.includeExistingFiles": "true",
}


def _autoload(path: str, schema):
    reader = spark.readStream.format("cloudFiles")  # noqa: F821
    for k, v in _CLOUD_FILES_OPTIONS.items():
        reader = reader.option(k, v)
    return reader.schema(schema).load(path)


@dlt.table(
    name="events_raw",
    comment="Append-only clickstream events captured from the landing volume.",
    partition_cols=["_ingest_date"],
    table_properties={"delta.appendOnly": "true", "quality": "bronze"},
)
@dlt.expect("event_id_present", "event_id IS NOT NULL")
@dlt.expect("event_ts_present", "event_ts IS NOT NULL")
def events_raw():
    return (
        _autoload(f"{LANDING_BASE}/events", EVENT_SCHEMA)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_date", F.to_date("_ingest_ts"))
    )


@dlt.table(
    name="users_raw",
    comment="User entity snapshots. SCD2 versioning happens in silver.",
    table_properties={"delta.appendOnly": "true", "quality": "bronze"},
)
@dlt.expect("user_id_present", "user_id IS NOT NULL")
def users_raw():
    return (
        _autoload(f"{LANDING_BASE}/users", USER_SCHEMA)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dlt.table(
    name="products_raw",
    comment="Product catalog snapshots. SCD2 versioning happens in silver.",
    table_properties={"delta.appendOnly": "true", "quality": "bronze"},
)
@dlt.expect("product_id_present", "product_id IS NOT NULL")
def products_raw():
    return (
        _autoload(f"{LANDING_BASE}/products", PRODUCT_SCHEMA)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
