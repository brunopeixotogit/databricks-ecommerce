"""Idempotently create the schema and volume that hold the FAISS index.

Layout:
    catalog: dev_main          (must already exist)
    schema:  ecom_artifacts    (created by this script)
    volume:  faiss             (created by this script)

Resulting Volume path:
    /Volumes/dev_main/ecom_artifacts/faiss/

Run once before the first build, or any time you need to reset the
artifact store. Safe to re-run — every step uses ``IF NOT EXISTS``.
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

CATALOG = os.getenv("FAISS_CATALOG", "dev_main")
SCHEMA = os.getenv("FAISS_SCHEMA", "ecom_artifacts")
VOLUME = os.getenv("FAISS_VOLUME", "faiss")
WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "29b258f884d1ac2c")


def _exec(w: WorkspaceClient, sql: str) -> None:
    logger.info("SQL: %s", sql.strip().replace("\n", " ")[:140])
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="30s"
    )
    sid = r.statement_id
    deadline = time.monotonic() + 60.0
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            raise RuntimeError(f"SQL timed out: {sid}")
        time.sleep(1.5)
        r = w.statement_execution.get_statement(sid)
    if r.status.state != StatementState.SUCCEEDED:
        msg = r.status.error.message if r.status.error else r.status.state
        raise RuntimeError(f"SQL failed: {msg}")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=CATALOG)
    parser.add_argument("--schema", default=SCHEMA)
    parser.add_argument("--volume", default=VOLUME)
    args = parser.parse_args(argv)

    w = WorkspaceClient()
    _exec(w, f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    _exec(w, f"CREATE VOLUME IF NOT EXISTS {args.catalog}.{args.schema}.{args.volume}")
    logger.info(
        "Ready: /Volumes/%s/%s/%s/", args.catalog, args.schema, args.volume
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
