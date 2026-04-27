"""IO helpers shared by all layers.

Centralises:
- canonical table references (``catalog.schema.name``)
- volume paths
- idempotent ``MERGE`` upserts
- schema / volume bootstrap

Keeping these in one module means a layer never builds a path or FQN
by hand, so renaming a schema is a single-line change.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class TableRef:
    """Fully-qualified Delta table reference."""

    catalog: str
    schema: str
    name: str

    @property
    def fqn(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.name}`"

    def __str__(self) -> str:  # convenience for logging
        return f"{self.catalog}.{self.schema}.{self.name}"


def volume_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    """Build a ``/Volumes/...`` path that works on Unity Catalog volumes."""
    sub = "/".join(p.strip("/") for p in parts if p)
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    return f"{base}/{sub}" if sub else base


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def ensure_volume(spark: SparkSession, catalog: str, schema: str, name: str) -> None:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{name}`")


def upsert(
    spark: SparkSession,
    target: TableRef,
    source: DataFrame,
    keys: Iterable[str],
    update_columns: Iterable[str] | None = None,
) -> None:
    """Idempotent upsert via Delta ``MERGE``.

    Creates the target table from the source schema if it does not yet
    exist. ``update_columns`` lets the caller restrict which columns
    are overwritten on match (useful when the source carries ingest
    metadata you do not want copied to the target).
    """
    if not spark.catalog.tableExists(target.fqn):
        source.limit(0).write.format("delta").saveAsTable(target.fqn)
    delta = DeltaTable.forName(spark, target.fqn)
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    builder = delta.alias("t").merge(source.alias("s"), cond)
    if update_columns:
        builder = builder.whenMatchedUpdate(
            set={c: f"s.`{c}`" for c in update_columns}
        )
    else:
        builder = builder.whenMatchedUpdateAll()
    builder.whenNotMatchedInsertAll().execute()


def overwrite(
    df: DataFrame,
    target: TableRef,
    partition_by: Iterable[str] | None = None,
) -> None:
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target.fqn)


def append(
    df: DataFrame,
    target: TableRef,
    partition_by: Iterable[str] | None = None,
) -> None:
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target.fqn)


def optimize(
    spark: SparkSession,
    target: TableRef,
    zorder: Iterable[str] | None = None,
) -> None:
    sql = f"OPTIMIZE {target.fqn}"
    if zorder:
        sql += " ZORDER BY (" + ", ".join(f"`{c}`" for c in zorder) + ")"
    spark.sql(sql)
