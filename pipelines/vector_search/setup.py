"""One-shot setup + refresh for the BricksShop Vector Search index.

What this script does (idempotent — safe to re-run):

  1. Builds an embedding model serving endpoint from a Databricks
     Foundation Model API system model (default: ``bge_large_en_v1_5``).
     Skipped if the endpoint already exists.

  2. Creates a regular Delta TABLE ``products_vs_source`` (CDF on) as
     a snapshot of ``dim_products_scd2`` filtered to current/active
     rows, with a ``doc_text`` column = ``"<name> | <category> | <brand>"``.
     ``dim_products_scd2`` is a DLT MATERIALIZED VIEW so it cannot be
     used directly as a Vector Search source — Delta-Sync needs a
     base table with CDF.

  3. Creates a Vector Search endpoint (``bricksshop_vs``) and a
     delta-sync index (``products_vs_index``) using
     **Databricks-managed embeddings** on ``doc_text``.

  4. Triggers a sync of the index.

Run it from the project root:

    DATABRICKS_HOST=...  DATABRICKS_TOKEN=...  python -m pipelines.vector_search.setup

Or from a Databricks notebook cell — the workspace identity is used
implicitly when no env vars are set. ``--refresh-only`` skips the
table/endpoint/index creation and just resyncs.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — single source of truth for every component.
# ---------------------------------------------------------------------------
CATALOG = os.getenv("VS_CATALOG", "dev_main")
SCHEMA = os.getenv("VS_SCHEMA", "ecom_dlt")
SOURCE_MV = os.getenv("VS_SOURCE_MV", f"{CATALOG}.{SCHEMA}.dim_products_scd2")
SOURCE_TABLE = os.getenv("VS_SOURCE_TABLE", f"{CATALOG}.{SCHEMA}.products_vs_source")
INDEX_NAME = os.getenv("VS_INDEX_NAME", f"{CATALOG}.{SCHEMA}.products_vs_index")
VS_ENDPOINT = os.getenv("VS_ENDPOINT", "bricksshop_vs")
EMBED_ENDPOINT = os.getenv("EMBED_ENDPOINT", "bricksshop-embed")
EMBED_MODEL_FULL = os.getenv("EMBED_MODEL_FULL", "system.ai.bge_small_en_v1_5")
EMBED_MODEL_VERSION = os.getenv("EMBED_MODEL_VERSION", "3")
WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "29b258f884d1ac2c")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _exec_sql(w: WorkspaceClient, sql: str) -> None:
    logger.info("SQL: %s", sql.replace("\n", " ")[:120])
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="30s"
    )
    sid = r.statement_id
    deadline = time.monotonic() + 120.0
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            raise RuntimeError(f"SQL timed out: {sid}")
        time.sleep(2.0)
        r = w.statement_execution.get_statement(sid)
    if r.status.state != StatementState.SUCCEEDED:
        msg = r.status.error.message if r.status.error else r.status.state
        raise RuntimeError(f"SQL failed: {msg}")


# ---------------------------------------------------------------------------
# 1. Embedding endpoint
# ---------------------------------------------------------------------------
def ensure_embed_endpoint(w: WorkspaceClient) -> None:
    try:
        ep = w.serving_endpoints.get(EMBED_ENDPOINT)
        logger.info(
            "Embedding endpoint %s exists (state=%s)",
            EMBED_ENDPOINT, ep.state.ready if ep.state else "?",
        )
        return
    except Exception:
        logger.info("Creating embedding endpoint %s ...", EMBED_ENDPOINT)

    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )
    w.serving_endpoints.create_and_wait(
        name=EMBED_ENDPOINT,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    name="embed",
                    entity_name=EMBED_MODEL_FULL,
                    entity_version=EMBED_MODEL_VERSION,
                    scale_to_zero_enabled=True,
                    workload_size="Small",
                )
            ]
        ),
    )
    logger.info("Embedding endpoint %s is READY", EMBED_ENDPOINT)


def wait_embed_ready(w: WorkspaceClient, timeout_s: int = 900) -> None:
    """Block until the endpoint reports READY (or raise on failure)."""
    deadline = time.monotonic() + timeout_s
    while True:
        ep = w.serving_endpoints.get(EMBED_ENDPOINT)
        ready = ep.state.ready.value if ep.state and ep.state.ready else "?"
        update = ep.state.config_update.value if ep.state and ep.state.config_update else "?"
        logger.info("  embed state: ready=%s config_update=%s", ready, update)
        if ready == "READY":
            return
        if time.monotonic() > deadline:
            raise RuntimeError(f"Embedding endpoint not READY after {timeout_s}s")
        time.sleep(20)


# ---------------------------------------------------------------------------
# 2. Source Delta table (CDF on, refreshed from the MV)
# ---------------------------------------------------------------------------
def refresh_source_table(w: WorkspaceClient) -> None:
    """Create or replace ``products_vs_source`` from the MV.

    INSERT OVERWRITE keeps the table id stable so the Vector Search
    index does not need to be recreated on every refresh.
    """
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (
            product_id   STRING NOT NULL,
            sku          STRING,
            name         STRING,
            category     STRING,
            brand        STRING,
            price        DOUBLE,
            currency     STRING,
            doc_text     STRING
        )
        USING DELTA
        TBLPROPERTIES (
            delta.enableChangeDataFeed = true,
            delta.feature.allowColumnDefaults = 'supported'
        )
    """
    _exec_sql(w, create_sql)

    insert_sql = f"""
        INSERT OVERWRITE {SOURCE_TABLE}
        SELECT
            product_id,
            sku,
            name,
            category,
            brand,
            price,
            currency,
            CONCAT_WS(' | ',
                COALESCE(name, ''),
                COALESCE(category, ''),
                COALESCE(brand, '')
            ) AS doc_text
        FROM {SOURCE_MV}
        WHERE is_current = TRUE AND active = TRUE
    """
    _exec_sql(w, insert_sql)
    logger.info("Refreshed %s from %s", SOURCE_TABLE, SOURCE_MV)


# ---------------------------------------------------------------------------
# 3. Vector Search endpoint + delta-sync index
# ---------------------------------------------------------------------------
def ensure_vs_endpoint() -> None:
    from databricks.vector_search.client import VectorSearchClient

    vsc = VectorSearchClient(disable_notice=True)
    try:
        vsc.get_endpoint(name=VS_ENDPOINT)
        logger.info("VS endpoint %s exists", VS_ENDPOINT)
        return
    except Exception:
        logger.info("Creating VS endpoint %s ...", VS_ENDPOINT)
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")
    logger.info("VS endpoint %s READY", VS_ENDPOINT)


def ensure_index() -> None:
    from databricks.vector_search.client import VectorSearchClient

    vsc = VectorSearchClient(disable_notice=True)
    try:
        idx = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)
        logger.info("Index %s exists; triggering sync", INDEX_NAME)
        idx.sync()
        return
    except Exception:
        logger.info("Creating delta-sync index %s ...", INDEX_NAME)

    vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT,
        source_table_name=SOURCE_TABLE,
        index_name=INDEX_NAME,
        primary_key="product_id",
        embedding_source_column="doc_text",
        embedding_model_endpoint_name=EMBED_ENDPOINT,
        pipeline_type="TRIGGERED",
    )
    logger.info("Index %s READY", INDEX_NAME)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh-only", action="store_true",
                        help="Skip endpoint/index creation; just refresh source + sync.")
    parser.add_argument("--wait-embed", action="store_true",
                        help="Block until the embedding endpoint is READY before continuing.")
    args = parser.parse_args(argv)

    w = WorkspaceClient()

    if not args.refresh_only:
        ensure_embed_endpoint(w)
    if args.wait_embed or not args.refresh_only:
        wait_embed_ready(w)

    refresh_source_table(w)

    if not args.refresh_only:
        ensure_vs_endpoint()
        ensure_index()
    else:
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient(disable_notice=True)
        idx = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)
        idx.sync()
        logger.info("Triggered re-sync of %s", INDEX_NAME)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
