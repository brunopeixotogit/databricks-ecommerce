"""Bronze ingestion via Auto Loader (``cloudFiles``).

Bronze is intentionally minimal: capture bytes faithfully, attach
ingest metadata, partition by ingest date. No business rules — that
is Silver's responsibility.

``schemaEvolutionMode=rescue`` ensures unexpected fields land in
``_rescued_data`` instead of failing the pipeline. The ``trigger``
parameter controls cost: ``availableNow`` processes any new files and
exits, which is ideal for Free Edition where keeping a stream alive
24/7 is wasteful.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.common.io import TableRef


def stream_events(
    spark: SparkSession,
    landing_path: str,
    checkpoint_path: str,
    target: TableRef,
    schema: StructType,
    trigger: str = "availableNow",
) -> None:
    """Ingest event JSON files from ``landing_path`` into ``target``."""
    raw = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(schema)
        .load(landing_path)
    )

    enriched = (
        raw
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_date", F.to_date("_ingest_ts"))
    )

    writer = (
        enriched.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("_ingest_date")
        .outputMode("append")
    )

    if trigger == "availableNow":
        writer = writer.trigger(availableNow=True)
    elif trigger == "continuous":
        writer = writer.trigger(processingTime="30 seconds")
    else:
        writer = writer.trigger(processingTime=trigger)

    writer.toTable(target.fqn).awaitTermination()


def stream_snapshot(
    spark: SparkSession,
    landing_path: str,
    checkpoint_path: str,
    schema: StructType,
    target: TableRef,
    trigger: str = "availableNow",
) -> None:
    """Generic Auto Loader for snapshot sources (users, products, etc)."""
    raw = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(schema)
        .load(landing_path)
    )

    enriched = (
        raw
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    writer = (
        enriched.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
    )

    if trigger == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger)

    writer.toTable(target.fqn).awaitTermination()
