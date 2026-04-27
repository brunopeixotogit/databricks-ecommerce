"""SCD Type 2 dimension for users.

Bronze ``users_raw`` accumulates successive snapshots of each user.
This builder collapses runs of unchanged attribute hashes into a
single version and emits a row per *change*, with ``valid_from`` /
``valid_to`` / ``is_current`` / ``version`` columns.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.common.io import TableRef, overwrite

# Columns whose change opens a new SCD2 version. updated_ts is excluded
# (it changes on every snapshot even when the data is identical).
TRACKED_COLS = ["email", "country", "city", "marketing_opt_in", "loyalty_tier"]


def build_dim_users_scd2(
    spark: SparkSession,
    bronze: TableRef,
    target: TableRef,
) -> None:
    bronze_df = spark.read.table(bronze.fqn).filter(F.col("user_id").isNotNull())

    hashed = bronze_df.withColumn(
        "_attr_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in TRACKED_COLS]), 256),
    )

    user_window = Window.partitionBy("user_id").orderBy("updated_ts")
    boundaries = (
        hashed
        .withColumn("_prev_hash", F.lag("_attr_hash").over(user_window))
        .filter(
            F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_attr_hash"))
        )
    )

    versions = (
        boundaries
        .withColumn("valid_from", F.col("updated_ts"))
        .withColumn("valid_to", F.lead("updated_ts").over(user_window))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version", F.row_number().over(user_window))
        .drop("_prev_hash", "_attr_hash")
    )

    overwrite(versions, target)
