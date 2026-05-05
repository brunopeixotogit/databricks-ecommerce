"""FAISS-backed semantic search with a Volume-resident index file.

Two cooperating responsibilities live here:

1. **Loader.** ``FaissProductIndex`` reads ``faiss_index_latest.txt``
   from the Volume, downloads the index file it points at, validates
   its embedding model + dim against the runtime ``Embedder``, and
   exposes ``search(query)``.

2. **Hot reload.** A background thread polls the pointer every
   ``poll_interval_s`` seconds. When the pointer's ``version`` changes,
   the new index is downloaded and atomically swapped in. In-flight
   queries hold a local reference to the previous index, so reload is
   non-blocking for readers.

The class is intentionally compatible (in shape) with
``ProductVectorIndex`` so ``ProductCatalog`` can treat the two as
interchangeable tiers in a fallback chain.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx
import numpy as np

from .embeddings import Embedder, EmbeddingError, get_default_embedder

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class FaissUnavailable(RuntimeError):
    """The pointer is missing, the index can't be loaded, or the model
    doesn't match. Caller should fall back to the next tier."""


# ---------------------------------------------------------------------------
# Hits — same shape as ProductVectorIndex.ProductHit so the catalog can
# uniformly map either source to ``Product`` rows.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FaissHit:
    product_id: str
    score: float

    def to_dict(self) -> dict[str, object]:
        return {"product_id": self.product_id, "score": self.score}


# ---------------------------------------------------------------------------
# Pointer + bytes helpers
# ---------------------------------------------------------------------------
@dataclass
class _LoadedIndex:
    version: str
    model_name: str
    dim: int
    n_vectors: int
    pids: list[str]                # row-aligned with the FAISS vectors
    index: object                  # faiss.IndexFlatIP


class FaissProductIndex:
    """Semantic search backed by a FAISS index in a Databricks Volume.

    Configuration via environment:
        DATABRICKS_HOST           — workspace host
        DATABRICKS_TOKEN          — PAT or M2M token (read-only is fine)
        FAISS_VOLUME_PATH         — absolute /Volumes/.../faiss path
                                    (default: /Volumes/dev_main/ecom_artifacts/faiss)
        FAISS_POLL_INTERVAL_S     — pointer poll interval (default 60s)
        SQL_WAREHOUSE_ID          — used to resolve product_id → Product
        EMBEDDINGS_TABLE          — Delta table with the (product_id, embedding) rows
                                    (used to recover the row→product_id mapping
                                     so we don't have to embed product names twice)
    """

    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        volume_path: str | None = None,
        poll_interval_s: float | None = None,
        embedder: Embedder | None = None,
        embeddings_table: str | None = None,
        warehouse_id: str | None = None,
    ) -> None:
        self.host = (host or os.getenv("DATABRICKS_HOST", "")).rstrip("/")
        self.token = token or os.getenv("DATABRICKS_TOKEN", "")
        self.volume_path = (
            volume_path or os.getenv(
                "FAISS_VOLUME_PATH", "/Volumes/dev_main/ecom_artifacts/faiss"
            )
        ).rstrip("/")
        self.pointer_path = f"{self.volume_path}/faiss_index_latest.txt"
        self.poll_interval_s = float(
            poll_interval_s or os.getenv("FAISS_POLL_INTERVAL_S", "60")
        )
        self.embedder = embedder or get_default_embedder()
        self.embeddings_table = embeddings_table or os.getenv(
            "EMBEDDINGS_TABLE", "dev_main.ecom_dlt.product_embeddings"
        )
        self.warehouse_id = warehouse_id or os.getenv("SQL_WAREHOUSE_ID", "")

        if not self.host or not self.token:
            raise FaissUnavailable("DATABRICKS_HOST/TOKEN required for FAISS loader")

        self._http = httpx.Client(
            base_url=self.host,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=60.0,
        )

        # Atomic-ish swap: hold both old and new together briefly during reload.
        self._loaded: _LoadedIndex | None = None
        self._swap_lock = threading.Lock()

        # Hot reload thread state.
        self._stop = threading.Event()
        self._poll_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Load the current index and start the background poller.

        Both initial load and the poller are best-effort: if the pointer
        is missing or the index is incompatible we leave ``self._loaded``
        as None and let ``search()`` raise ``FaissUnavailable``.
        """
        try:
            self._reload(reason="initial")
        except FaissUnavailable as exc:
            logger.warning("FAISS not available at startup: %s", exc)

        if self._poll_thread is not None:
            return  # already running
        self._stop.clear()
        t = threading.Thread(
            target=self._poll_loop, name="faiss-poller", daemon=True
        )
        t.start()
        self._poll_thread = t
        logger.info("FAISS hot-reload poller started (every %.0fs)", self.poll_interval_s)

    def close(self) -> None:
        self._stop.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2.0)
            self._poll_thread = None
        try:
            self._http.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Public query API — shape-compatible with ProductVectorIndex.
    # ------------------------------------------------------------------
    def is_ready(self) -> bool:
        return self._loaded is not None

    def info(self) -> dict[str, object]:
        loaded = self._loaded
        if loaded is None:
            return {"ready": False}
        return {
            "ready":                True,
            "version":              loaded.version,
            "embedding_model_name": loaded.model_name,
            "embedding_dim":        loaded.dim,
            "n_vectors":            loaded.n_vectors,
            "volume_path":          self.volume_path,
        }

    def search(
        self,
        query_text: str,
        *,
        num_results: int = 5,
        oversample: int = 5,
    ) -> list[FaissHit]:
        """Return the top-``num_results`` product_ids ranked by cosine similarity.

        Filtering by category / max_price is done at the SQL join step
        in ``ProductCatalog`` because FAISS itself doesn't carry those
        attributes. We oversample so the post-filter still has enough
        candidates to satisfy ``num_results`` after the join.
        """
        if not query_text or not query_text.strip():
            return []
        loaded = self._loaded
        if loaded is None:
            raise FaissUnavailable("FAISS index not loaded")

        try:
            qvec = self.embedder.encode_one(query_text)
        except EmbeddingError as exc:
            raise FaissUnavailable(f"embedder failed: {exc}") from exc

        if qvec.shape[1] != loaded.dim:
            raise FaissUnavailable(
                f"query dim {qvec.shape[1]} != index dim {loaded.dim}"
            )

        k = max(1, min(int(num_results) * max(1, oversample), loaded.n_vectors))
        try:
            scores, idxs = loaded.index.search(qvec, k)
        except Exception as exc:
            raise FaissUnavailable(f"FAISS search failed: {exc}") from exc

        hits: list[FaissHit] = []
        for s, i in zip(scores[0], idxs[0]):
            if i < 0 or i >= len(loaded.pids):
                continue
            hits.append(FaissHit(product_id=loaded.pids[int(i)], score=float(s)))
        return hits

    # ------------------------------------------------------------------
    # Reload mechanics
    # ------------------------------------------------------------------
    def _read_pointer(self) -> dict | None:
        url = f"/api/2.0/fs/files{self.pointer_path}"
        try:
            r = self._http.get(url)
        except httpx.HTTPError as exc:
            raise FaissUnavailable(f"pointer GET failed: {exc}") from exc
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise FaissUnavailable(f"pointer GET {r.status_code}: {r.text[:200]}")
        try:
            return json.loads(r.content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FaissUnavailable(f"pointer JSON malformed: {exc}") from exc

    def _download_index_file(self, fname: str) -> bytes:
        abs_path = f"{self.volume_path}/{fname}"
        url = f"/api/2.0/fs/files{abs_path}"
        try:
            r = self._http.get(url)
        except httpx.HTTPError as exc:
            raise FaissUnavailable(f"index GET failed: {exc}") from exc
        if r.status_code >= 400:
            raise FaissUnavailable(f"index GET {r.status_code}: {abs_path}")
        return r.content

    def _read_pids_in_index_order(self) -> list[str]:
        """Read product_ids from the embeddings table in the order the
        FAISS index was built.

        The build script orders by ``product_id`` ascending and inserts
        in that order; ``read_files()`` preserves that order on a fresh
        ``CREATE OR REPLACE TABLE``. We re-derive that ordering with an
        explicit ``ORDER BY product_id`` so behaviour doesn't depend on
        the loader's read plan.
        """
        if not self.warehouse_id:
            raise FaissUnavailable("SQL_WAREHOUSE_ID required to map FAISS rows to products")
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        w = WorkspaceClient()
        sql = f"SELECT product_id FROM {self.embeddings_table} ORDER BY product_id"
        r = w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=sql, wait_timeout="30s"
        )
        sid = r.statement_id
        deadline = time.monotonic() + 60.0
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                raise FaissUnavailable(f"product_id read timed out: {sid}")
            time.sleep(1.5)
            r = w.statement_execution.get_statement(sid)
        if r.status.state != StatementState.SUCCEEDED:
            msg = r.status.error.message if r.status.error else r.status.state
            raise FaissUnavailable(f"product_id read failed: {msg}")
        rows = (r.result.data_array if r.result else None) or []
        return [row[0] for row in rows]

    def _reload(self, reason: str) -> bool:
        """Load (or replace) the in-memory index. Returns True if changed."""
        ptr = self._read_pointer()
        if not ptr:
            raise FaissUnavailable(f"pointer missing at {self.pointer_path}")
        version = str(ptr.get("version", ""))
        fname = str(ptr.get("filename", ""))
        if not version or not fname:
            raise FaissUnavailable(f"pointer missing fields: {ptr}")

        loaded = self._loaded
        if loaded is not None and loaded.version == version:
            return False

        model_name = str(ptr.get("embedding_model_name", ""))
        if model_name and model_name != self.embedder.model_name:
            raise FaissUnavailable(
                f"model mismatch: index={model_name} runtime={self.embedder.model_name}"
            )
        dim = int(ptr.get("embedding_dim") or 0)
        if dim and dim != self.embedder.dim:
            raise FaissUnavailable(
                f"dim mismatch: index={dim} runtime={self.embedder.dim}"
            )
        n_vectors = int(ptr.get("n_vectors") or 0)

        body = self._download_index_file(fname)
        with tempfile.NamedTemporaryFile(suffix=".idx", delete=False) as tmp:
            tmp.write(body)
            tmp_path = tmp.name
        try:
            import faiss
            index = faiss.read_index(tmp_path)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        if index.ntotal == 0:
            raise FaissUnavailable(f"index {fname} has 0 vectors")

        # Pull the row→product_id mapping from the embeddings table.
        pids = self._read_pids_in_index_order()
        if len(pids) != index.ntotal:
            raise FaissUnavailable(
                f"row count mismatch: pids={len(pids)} faiss={index.ntotal}"
            )

        new_loaded = _LoadedIndex(
            version=version,
            model_name=model_name or self.embedder.model_name,
            dim=dim or self.embedder.dim,
            n_vectors=n_vectors or index.ntotal,
            pids=pids,
            index=index,
        )
        with self._swap_lock:
            old = self._loaded
            self._loaded = new_loaded
        logger.info(
            "FAISS index loaded (%s): version=%s n=%d  [%s]",
            reason, new_loaded.version, new_loaded.n_vectors,
            "first load" if old is None else f"replaced v{old.version}",
        )
        return True

    def _poll_loop(self) -> None:
        last_err: str | None = None
        while not self._stop.is_set():
            self._stop.wait(self.poll_interval_s)
            if self._stop.is_set():
                break
            try:
                self._reload(reason="poll")
                last_err = None
            except FaissUnavailable as exc:
                # Don't spam logs if the same problem persists.
                msg = str(exc)
                if msg != last_err:
                    logger.warning("FAISS poll: %s", msg)
                    last_err = msg
            except Exception as exc:  # defensive — never let the thread die
                logger.exception("FAISS poll: unexpected error: %s", exc)
