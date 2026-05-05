"""Build a FAISS index from the BricksShop product catalog.

End-to-end flow (idempotent — safe to re-run):

    Delta dim_products_scd2 (current/active rows)
        │  Statement Execution API
        ▼
    Embedder (sentence-transformers/all-MiniLM-L6-v2, L2-normalised)
        │
        ├──▶ JSONL staged on Volume → COPY INTO dev_main.ecom_dlt.product_embeddings
        │      (columns include embedding_model_name + embedding_version
        │       so the contract is auditable from SQL alone)
        │
        └──▶ faiss.IndexFlatIP built from the same vectors
                │
                ▼
            /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_v{ts}.idx
                │
                ▼  (only after the .idx upload succeeds)
            /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_latest.txt
                = JSON pointer { filename, version, model, dim, n, built_at }

The pointer is uploaded LAST so a backend that polls during a rebuild
never sees a pointer to a not-yet-uploaded index file. Versioned
``faiss_index_v{ts}.idx`` files accumulate; ``--keep-versions N`` (or
the ``FAISS_KEEP_VERSIONS`` env var) prunes anything older than the
N most recent.

Run modes
---------
* Local CLI:                ``python -m pipelines.faiss.build``
* Databricks notebook:      see ``notebooks/faiss_rebuild.py``
* Databricks Job:           wired into ``databricks.yml`` as
                            ``bricksshop-faiss-rebuild`` (scheduled).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

# Build-time imports done lazily inside main so an `import build` from a
# linter doesn't drag faiss in.

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CATALOG = os.getenv("FAISS_CATALOG", "dev_main")
SCHEMA_DLT = os.getenv("PRODUCT_SCHEMA", "ecom_dlt")
SCHEMA_ARTIFACTS = os.getenv("FAISS_SCHEMA", "ecom_artifacts")
VOLUME = os.getenv("FAISS_VOLUME", "faiss")

PRODUCT_TABLE = os.getenv(
    "PRODUCT_TABLE", f"{CATALOG}.{SCHEMA_DLT}.dim_products_scd2"
)
EMBEDDINGS_TABLE = os.getenv(
    "EMBEDDINGS_TABLE", f"{CATALOG}.{SCHEMA_DLT}.product_embeddings"
)

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_ARTIFACTS}/{VOLUME}"
STAGING_PATH = f"{VOLUME_PATH}/staging"
POINTER_FILE = f"{VOLUME_PATH}/faiss_index_latest.txt"

WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "29b258f884d1ac2c")
KEEP_VERSIONS = int(os.getenv("FAISS_KEEP_VERSIONS", "5"))


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _exec_sql(
    w: WorkspaceClient,
    sql: str,
    params: list[StatementParameterListItem] | None = None,
) -> list[list[str]]:
    logger.debug("SQL: %s", sql.strip().replace("\n", " ")[:200])
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        parameters=params or [],
        wait_timeout="30s",
    )
    sid = r.statement_id
    deadline = time.monotonic() + 600.0
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            raise RuntimeError(f"SQL timed out: {sid}")
        time.sleep(2.0)
        r = w.statement_execution.get_statement(sid)
    if r.status.state != StatementState.SUCCEEDED:
        msg = r.status.error.message if r.status.error else r.status.state
        raise RuntimeError(f"SQL failed: {msg}\n{sql[:300]}")
    if not r.result or not r.result.data_array:
        return []
    return r.result.data_array


def _files_client(w: WorkspaceClient) -> httpx.Client:
    """Tiny httpx wrapper around the Volumes Files API.

    We could use ``w.files`` from the SDK, but binary upload semantics
    are clearer here and the auth path matches the rest of the project.
    """
    cfg = w.config
    return httpx.Client(
        base_url=cfg.host.rstrip("/"),
        headers={"Authorization": f"Bearer {cfg.token}"} if cfg.token else {},
        timeout=120.0,
    )


def _put_file(client: httpx.Client, abs_path: str, body: bytes,
              content_type: str = "application/octet-stream") -> None:
    """PUT raw bytes to a Volume path, overwriting any existing file."""
    url = f"/api/2.0/fs/files{abs_path}"
    r = client.put(
        url, params={"overwrite": "true"}, content=body,
        headers={"Content-Type": content_type},
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Volume PUT failed ({r.status_code}) {abs_path}: {r.text[:300]}")


def _list_dir(client: httpx.Client, abs_path: str) -> list[dict]:
    """List directory entries via the Files API. Returns [] if path missing."""
    r = client.get(f"/api/2.0/fs/directories{abs_path}")
    if r.status_code == 404:
        return []
    if r.status_code >= 400:
        raise RuntimeError(f"Volume LIST failed ({r.status_code}): {r.text[:300]}")
    return r.json().get("contents", []) or []


def _delete_file(client: httpx.Client, abs_path: str) -> None:
    r = client.delete(f"/api/2.0/fs/files{abs_path}")
    if r.status_code >= 400 and r.status_code != 404:
        logger.warning("Volume DELETE failed (%s): %s", r.status_code, abs_path)


# ---------------------------------------------------------------------------
# Read products
# ---------------------------------------------------------------------------
def read_products(w: WorkspaceClient) -> list[dict]:
    """Return current/active products with a synthesised ``doc_text``.

    ``doc_text`` is what we hand to the embedder, so its shape is the
    contract between this script and any other component that wants to
    embed a query the same way (the FastAPI backend uses just the user's
    chat message, but we keep the doc-side concatenation here so future
    re-rankers can compare apples to apples).
    """
    sql = f"""
        SELECT
            product_id,
            COALESCE(name, '')      AS name,
            COALESCE(category, '')  AS category,
            COALESCE(brand, '')     AS brand,
            CONCAT_WS(' | ',
                COALESCE(name, ''),
                COALESCE(category, ''),
                COALESCE(brand, '')
            ) AS doc_text
        FROM {PRODUCT_TABLE}
        WHERE is_current = TRUE AND active = TRUE
        ORDER BY product_id
    """
    rows = _exec_sql(w, sql)
    products = [
        {"product_id": r[0], "name": r[1], "category": r[2], "brand": r[3], "doc_text": r[4]}
        for r in rows
    ]
    logger.info("Read %d products from %s", len(products), PRODUCT_TABLE)
    return products


# ---------------------------------------------------------------------------
# Write embeddings → Delta (via JSONL staged on Volume + COPY INTO)
# ---------------------------------------------------------------------------
def write_embeddings_delta(
    w: WorkspaceClient,
    client: httpx.Client,
    products: list[dict],
    vectors: np.ndarray,
    model_name: str,
    version: str,
) -> None:
    """Replace the embeddings table from a freshly staged JSONL file.

    We use ``read_files()`` + ``CREATE OR REPLACE TABLE`` rather than
    multi-row ``INSERT ... VALUES`` because 4k float arrays per query
    blow past warehouse query-size limits quickly.
    """
    if len(products) != vectors.shape[0]:
        raise RuntimeError(
            f"Length mismatch: products={len(products)} vectors={vectors.shape[0]}"
        )

    embed_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    lines: list[str] = []
    for p, v in zip(products, vectors):
        lines.append(json.dumps({
            "product_id":            p["product_id"],
            "doc_text":              p["doc_text"],
            "embedding":             [float(x) for x in v],
            "embedding_model_name":  model_name,
            "embedding_version":     version,
            "embed_ts":              embed_ts,
        }))
    body = ("\n".join(lines) + "\n").encode("utf-8")

    staged_name = f"embeddings_{version}_{uuid.uuid4().hex[:6]}.jsonl"
    staged_abs = f"{STAGING_PATH}/{staged_name}"
    _put_file(client, staged_abs, body, content_type="application/x-ndjson")
    logger.info("Staged %d rows → %s (%.1f KB)", len(lines), staged_abs, len(body) / 1024)

    create_sql = f"""
        CREATE OR REPLACE TABLE {EMBEDDINGS_TABLE} AS
        SELECT
            product_id,
            doc_text,
            embedding,
            embedding_model_name,
            embedding_version,
            CAST(embed_ts AS TIMESTAMP) AS embed_ts
        FROM read_files(
            '{staged_abs}',
            format => 'json',
            schema => 'product_id STRING NOT NULL,
                       doc_text STRING,
                       embedding ARRAY<FLOAT>,
                       embedding_model_name STRING,
                       embedding_version STRING,
                       embed_ts STRING'
        )
    """
    _exec_sql(w, create_sql)
    logger.info("Refreshed %s (%d rows, model=%s, version=%s)",
                EMBEDDINGS_TABLE, len(products), model_name, version)

    # Best-effort: clean up the staged file. A leftover does no harm but
    # keeps the staging dir tidy across many rebuilds.
    _delete_file(client, staged_abs)


# ---------------------------------------------------------------------------
# Build FAISS + upload + pointer + retention
# ---------------------------------------------------------------------------
def build_faiss(vectors: np.ndarray) -> "object":
    import faiss  # local import: heavy native dep
    dim = vectors.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(vectors)
    if index.ntotal != vectors.shape[0]:
        raise RuntimeError(
            f"FAISS rejected vectors: ntotal={index.ntotal} expected={vectors.shape[0]}"
        )
    logger.info("Built IndexFlatIP (dim=%d, n=%d)", dim, index.ntotal)
    return index


def upload_index(client: httpx.Client, index, version: str) -> str:
    import faiss
    fname = f"faiss_index_v{version}.idx"
    abs_path = f"{VOLUME_PATH}/{fname}"
    with tempfile.NamedTemporaryFile(suffix=".idx", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        faiss.write_index(index, tmp_path)
        body = Path(tmp_path).read_bytes()
        _put_file(client, abs_path, body)
        logger.info("Uploaded %s (%.1f KB)", abs_path, len(body) / 1024)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
    return fname


def update_pointer(
    client: httpx.Client,
    fname: str,
    version: str,
    model_name: str,
    dim: int,
    n_vectors: int,
) -> None:
    pointer = {
        "filename":             fname,
        "version":              version,
        "embedding_model_name": model_name,
        "embedding_dim":        dim,
        "n_vectors":            n_vectors,
        "built_at":             datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    body = json.dumps(pointer, indent=2).encode("utf-8")
    _put_file(client, POINTER_FILE, body, content_type="application/json")
    logger.info("Pointer updated: %s → %s", POINTER_FILE, fname)


def prune_old_versions(client: httpx.Client, keep: int) -> None:
    if keep <= 0:
        return
    entries = _list_dir(client, VOLUME_PATH)
    versioned = [
        e for e in entries
        if isinstance(e, dict)
        and (e.get("name", "")).startswith("faiss_index_v")
        and (e.get("name", "")).endswith(".idx")
    ]
    # Sort newest first by name (timestamp embedded after 'v').
    versioned.sort(key=lambda e: e["name"], reverse=True)
    to_delete = versioned[keep:]
    for e in to_delete:
        path = e.get("path") or f"{VOLUME_PATH}/{e['name']}"
        logger.info("Pruning old index: %s", path)
        _delete_file(client, path)


# ---------------------------------------------------------------------------
# Top-level entry point — callable from CLI, notebook, or Databricks Job
# ---------------------------------------------------------------------------
def build(
    keep_versions: int = KEEP_VERSIONS,
    show_progress: bool = False,
) -> dict:
    """Run the full rebuild and return a summary dict.

    Idempotent. Safe to run concurrently in different sessions —
    versioned filenames avoid collisions; the pointer write is the
    last side-effect, so a partial run never poisons the latest pointer.
    """
    # Heavy imports kept inside the function so module import stays fast.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from web.backend.embeddings import Embedder

    embedder = Embedder()
    model_name = embedder.model_name
    dim = embedder.dim
    version = str(int(time.time()))

    w = WorkspaceClient()
    products = read_products(w)
    if not products:
        raise RuntimeError("No products to embed — refusing to publish an empty index.")

    vectors = embedder.encode(
        [p["doc_text"] for p in products],
        show_progress=show_progress,
    )
    if vectors.shape[1] != dim:
        raise RuntimeError(
            f"Embedder produced dim {vectors.shape[1]}, expected {dim}"
        )

    with _files_client(w) as client:
        write_embeddings_delta(w, client, products, vectors, model_name, version)

        index = build_faiss(vectors)
        fname = upload_index(client, index, version)
        update_pointer(
            client, fname, version, model_name, dim, len(products)
        )
        prune_old_versions(client, keep_versions)

    summary = {
        "version":              version,
        "filename":             fname,
        "embeddings_table":     EMBEDDINGS_TABLE,
        "volume_path":          VOLUME_PATH,
        "embedding_model_name": model_name,
        "embedding_dim":        dim,
        "n_vectors":            len(products),
    }
    logger.info("BUILD OK: %s", json.dumps(summary))
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep-versions", type=int, default=KEEP_VERSIONS)
    parser.add_argument("--progress", action="store_true",
                        help="Show a progress bar while encoding (CLI only).")
    args = parser.parse_args(argv)
    try:
        build(keep_versions=args.keep_versions, show_progress=args.progress)
    except Exception as exc:
        logger.exception("BUILD FAILED: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
