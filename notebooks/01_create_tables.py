# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Create Bronze Delta tables
# MAGIC
# MAGIC Explicitly pre-creates the three Bronze raw tables (`users_raw`,
# MAGIC `products_raw`, `events_raw`) so that schemas are locked in *before*
# MAGIC Auto Loader writes the first row.
# MAGIC
# MAGIC Why pre-create instead of letting Auto Loader infer-on-first-write:
# MAGIC
# MAGIC - Schema is a **contract** with the producer. We declare it once,
# MAGIC   here, and `schemaEvolutionMode = rescue` keeps drift out of the
# MAGIC   typed columns.
# MAGIC - Partitioning (`_ingest_date` on `events_raw`) is locked from row 0.
# MAGIC - Table comments / properties / `delta.appendOnly` are set up front,
# MAGIC   not retro-fitted.
# MAGIC - Idempotent: `CREATE TABLE IF NOT EXISTS` — safe to leave in the
# MAGIC   Workflow DAG.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------
dbutils.widgets.text("catalog",       "main")
dbutils.widgets.text("bronze_schema", "ecom_bronze")

CATALOG = dbutils.widgets.get("catalog")
BRONZE  = dbutils.widgets.get("bronze_schema")

# COMMAND ----------
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DoubleType, IntegerType, BooleanType,
    DateType, MapType,
)

from src.common.schemas import (
    EVENT_SCHEMA, USER_SCHEMA, PRODUCT_SCHEMA, SCHEMA_VERSION,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze metadata columns
# MAGIC
# MAGIC Every Bronze table carries the same three columns appended to the
# MAGIC source-faithful schema. They are populated by the Auto Loader job in
# MAGIC `notebooks/20_bronze.py`; pre-declaring them here means the table
# MAGIC schema matches the writer schema exactly — no `mergeSchema` surprise
# MAGIC on the very first micro-batch.

# COMMAND ----------
BRONZE_META_FIELDS = [
    StructField("_ingest_ts",   TimestampType(), False),
    StructField("_source_file", StringType(),    True),
]
EVENTS_META_FIELDS = BRONZE_META_FIELDS + [
    StructField("_ingest_date", DateType(), False),  # partition column
]

EVENTS_RAW_SCHEMA   = StructType(EVENT_SCHEMA.fields   + EVENTS_META_FIELDS)
USERS_RAW_SCHEMA    = StructType(USER_SCHEMA.fields    + BRONZE_META_FIELDS)
PRODUCTS_RAW_SCHEMA = StructType(PRODUCT_SCHEMA.fields + BRONZE_META_FIELDS)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DDL helper
# MAGIC
# MAGIC `StructType.toDDL()` emits a comma-separated `col TYPE` list that we
# MAGIC drop directly into a `CREATE TABLE` statement. NOT NULL constraints
# MAGIC are added explicitly: `toDDL()` does not preserve nullability.

# COMMAND ----------
def _column_ddl(schema: StructType) -> str:
    parts = []
    for f in schema.fields:
        nn = "" if f.nullable else " NOT NULL"
        parts.append(f"  `{f.name}` {f.dataType.simpleString()}{nn}")
    return ",\n".join(parts)


def create_bronze_table(
    fqn: str,
    schema: StructType,
    *,
    partition_cols: list[str] | None = None,
    comment: str = "",
) -> None:
    """Idempotent Bronze table DDL.

    Properties:
      - delta.appendOnly = true        (Bronze is a replay log)
      - delta.columnMapping.mode=name  (safe rename / drop later)
      - delta.minReaderVersion / minWriterVersion bumped accordingly
      - delta.autoOptimize.optimizeWrite + autoCompact enabled
    """
    cols = _column_ddl(schema)
    part = f"PARTITIONED BY ({', '.join(f'`{c}`' for c in partition_cols)})" if partition_cols else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
        {cols}
        )
        USING DELTA
        {part}
        COMMENT '{comment}'
        TBLPROPERTIES (
            'delta.appendOnly'                  = 'true',
            'delta.columnMapping.mode'          = 'name',
            'delta.minReaderVersion'            = '2',
            'delta.minWriterVersion'            = '5',
            'delta.autoOptimize.optimizeWrite'  = 'true',
            'delta.autoOptimize.autoCompact'    = 'true',
            'pipeline.layer'                    = 'bronze',
            'pipeline.schema_version'           = '{SCHEMA_VERSION}'
        )
    """)
    print(f"[ok] {fqn}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the three Bronze tables

# COMMAND ----------
create_bronze_table(
    fqn=f"`{CATALOG}`.`{BRONZE}`.`users_raw`",
    schema=USERS_RAW_SCHEMA,
    comment="Raw user snapshots landed by Auto Loader. Append-only.",
)

create_bronze_table(
    fqn=f"`{CATALOG}`.`{BRONZE}`.`products_raw`",
    schema=PRODUCTS_RAW_SCHEMA,
    comment="Raw product catalog snapshots landed by Auto Loader. Append-only.",
)

create_bronze_table(
    fqn=f"`{CATALOG}`.`{BRONZE}`.`events_raw`",
    schema=EVENTS_RAW_SCHEMA,
    partition_cols=["_ingest_date"],
    comment="Raw clickstream events landed by Auto Loader. Append-only; partitioned by ingest date.",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------
display(spark.sql(f"""
    SELECT table_name, table_type, created
    FROM   {CATALOG}.information_schema.tables
    WHERE  table_schema = '{BRONZE}'
    AND    table_name IN ('users_raw','products_raw','events_raw')
    ORDER  BY table_name
"""))

# COMMAND ----------
display(spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{BRONZE}.events_raw"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Smoke-test inserts (synthetic, one row each)
# MAGIC
# MAGIC Demonstrates that the typed schema and the writer agree. Each insert
# MAGIC builds a DataFrame with the exact pre-declared schema (no
# MAGIC `inferSchema`) and appends. Re-running this cell adds a row each
# MAGIC time — that's expected for an append-only Bronze table.

# COMMAND ----------
# --- users_raw --------------------------------------------------------------
now = datetime.now(timezone.utc)

users_rows = [(
    "u_demo_0001",                 # user_id
    "demo@example.com",            # email
    "Demo",                        # first_name
    "User",                        # last_name
    "BR",                          # country
    "São Paulo",                   # city
    now,                           # signup_ts
    True,                          # marketing_opt_in
    "silver",                      # loyalty_tier
    now,                           # updated_ts
    SCHEMA_VERSION,                # schema_version
    now,                           # _ingest_ts
    "/Volumes/main/ecom_bronze/landing/users/demo.json",  # _source_file
)]

(spark.createDataFrame(users_rows, schema=USERS_RAW_SCHEMA)
    .write.format("delta").mode("append")
    .saveAsTable(f"{CATALOG}.{BRONZE}.users_raw"))

# COMMAND ----------
# --- products_raw -----------------------------------------------------------
products_rows = [(
    "p_demo_0001",                 # product_id
    "SKU-DEMO-001",                # sku
    "Demo Smartphone X",           # name
    "electronics",                 # category
    "smartphone",                  # subcategory
    "Pixelon",                     # brand
    899.00,                        # price
    "USD",                         # currency
    True,                          # active
    now,                           # updated_ts
    SCHEMA_VERSION,                # schema_version
    now,                           # _ingest_ts
    "/Volumes/main/ecom_bronze/landing/products/demo.json",
)]

(spark.createDataFrame(products_rows, schema=PRODUCTS_RAW_SCHEMA)
    .write.format("delta").mode("append")
    .saveAsTable(f"{CATALOG}.{BRONZE}.products_raw"))

# COMMAND ----------
# --- events_raw -------------------------------------------------------------
events_rows = [(
    "e_demo_0001",                 # event_id
    "page_view",                   # event_type
    now,                           # event_ts
    "u_demo_0001",                 # user_id
    "s_demo_0001",                 # session_id
    "mobile",                      # device
    "Mozilla/5.0 (sim)",           # user_agent
    "10.0.0.1",                    # ip
    "BR",                          # country
    "/p/p_demo_0001",              # page_url
    "google",                      # referrer
    "p_demo_0001",                 # product_id
    "electronics",                 # category
    899.00,                        # price
    1,                             # quantity
    None,                          # cart_id
    None,                          # order_id
    None,                          # payment_method
    None,                          # discount_code
    {"campaign": "demo"},          # properties
    SCHEMA_VERSION,                # schema_version
    now,                           # _ingest_ts
    "/Volumes/main/ecom_bronze/landing/events/dt=demo/demo.json.gz",
    now.date(),                    # _ingest_date  (partition)
)]

(spark.createDataFrame(events_rows, schema=EVENTS_RAW_SCHEMA)
    .write.format("delta").mode("append")
    .saveAsTable(f"{CATALOG}.{BRONZE}.events_raw"))

# COMMAND ----------
# MAGIC %md ## Row counts after smoke insert

# COMMAND ----------
display(spark.sql(f"""
    SELECT 'users_raw'    AS table, COUNT(*) AS rows FROM {CATALOG}.{BRONZE}.users_raw
    UNION ALL
    SELECT 'products_raw',          COUNT(*)        FROM {CATALOG}.{BRONZE}.products_raw
    UNION ALL
    SELECT 'events_raw',            COUNT(*)        FROM {CATALOG}.{BRONZE}.events_raw
"""))
