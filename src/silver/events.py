"""Silver events: dedupe, type-cleanse, sessionize.

Sessionization derives ``session_id_silver`` from event-time gaps
because the producer's ``session_id`` may not survive distributed
shuffles in a real Kafka topology (different shards, late events). The
producer's value is preserved as ``session_id_raw`` for debugging.

Idempotent: keyed on ``event_id`` via Delta MERGE. Re-running over the
same Bronze window does not produce duplicates.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.common.io import TableRef, upsert

VALID_EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "abandon_cart"]


def build_silver_events(
    spark: SparkSession,
    bronze: TableRef,
    target: TableRef,
    inactivity_minutes: int = 30,
) -> None:
    bronze_df = spark.read.table(bronze.fqn)

    cleaned = (
        bronze_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .filter(F.col("event_ts").isNotNull())
    )

    # Dedupe on event_id, keeping the latest ingest of each.
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
    deduped = (
        cleaned
        .withColumn("_rk", F.row_number().over(dedup_window))
        .filter("_rk = 1")
        .drop("_rk")
    )

    # Sessionize: a new session begins after `inactivity_minutes` of idleness.
    user_window = Window.partitionBy("user_id").orderBy("event_ts")
    sessionized = (
        deduped
        .withColumn("_prev_ts", F.lag("event_ts").over(user_window))
        .withColumn(
            "_gap_min",
            (F.col("event_ts").cast("long") - F.col("_prev_ts").cast("long")) / 60.0,
        )
        .withColumn(
            "_new_session",
            F.when(
                F.col("_prev_ts").isNull() | (F.col("_gap_min") > inactivity_minutes),
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

    upsert(spark, target, sessionized, keys=["event_id"])
