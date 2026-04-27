"""Conversion funnel: view -> cart -> purchase per session, aggregated daily.

Defines the standard e-commerce funnel rates that go on the executive
dashboard. Computed at session grain (not event grain) because a
session is the natural unit of purchase intent.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite


def build_funnel(
    spark: SparkSession,
    silver_events: TableRef,
    target: TableRef,
) -> None:
    events = spark.read.table(silver_events.fqn)

    sessions = (
        events
        .groupBy(
            F.to_date("event_ts").alias("event_date"),
            F.col("session_id_silver"),
        )
        .agg(
            F.max((F.col("event_type") == "page_view").cast("int")).alias("viewed"),
            F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("carted"),
            F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased"),
            F.max((F.col("event_type") == "abandon_cart").cast("int")).alias("abandoned"),
        )
    )

    funnel = (
        sessions
        .groupBy("event_date")
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

    overwrite(funnel, target, partition_by=["event_date"])
