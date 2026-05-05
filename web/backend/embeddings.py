"""Sentence-transformer embedder shared between the FAISS build job and
the FastAPI query path.

The single source of truth for *which* model and *which* dimension we
use lives here, in ``DEFAULT_MODEL`` and ``DEFAULT_DIM``. The build job
writes both of those into the FAISS pointer file so the runtime can
refuse to load an index that disagrees with the model the backend has
loaded — a guardrail against the "we changed the model but forgot to
rebuild" foot-gun.

Embeddings are L2-normalised so a FAISS ``IndexFlatIP`` (inner-product)
gives cosine similarity, which is what every downstream consumer
(re-rankers, threshold filters) expects.

This module is intentionally framework-light: ``sentence-transformers``
+ ``numpy``. No torch imports at module load time — the model is built
lazily on first use so a backend that never receives a chat request
doesn't pay the load cost.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Iterable

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.getenv("EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
DEFAULT_DIM = int(os.getenv("EMBED_DIM", "384"))
DEFAULT_BATCH = int(os.getenv("EMBED_BATCH", "64"))


class EmbeddingError(RuntimeError):
    """Model load failed or encode produced an unexpected shape."""


class Embedder:
    """Lazy, thread-safe wrapper around a SentenceTransformer model.

    Used in two places that must agree on tokenisation, model weights,
    and output dimension:

    * ``pipelines/faiss/build.py`` — encodes the entire product corpus.
    * ``web/backend/faiss_index.py`` — encodes the user's chat query.

    Keep both instances pointing at the same ``DEFAULT_MODEL`` / env var.
    """

    def __init__(self, model_name: str | None = None, expected_dim: int | None = None) -> None:
        self.model_name = model_name or DEFAULT_MODEL
        self.expected_dim = expected_dim or DEFAULT_DIM
        self._model = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    @property
    def dim(self) -> int:
        return self.expected_dim

    def _ensure_model(self):
        if self._model is not None:
            return self._model
        with self._lock:
            if self._model is not None:
                return self._model
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:
                raise EmbeddingError(
                    "sentence-transformers is not installed. "
                    "Add it to web/backend/requirements.txt."
                ) from exc
            logger.info("Loading sentence-transformer %s ...", self.model_name)
            self._model = SentenceTransformer(self.model_name)
            try:
                actual_dim = int(self._model.get_sentence_embedding_dimension())
            except Exception:
                actual_dim = self.expected_dim
            if actual_dim != self.expected_dim:
                raise EmbeddingError(
                    f"Embedding dim mismatch: model={actual_dim} expected={self.expected_dim}"
                )
            logger.info("Embedder ready (model=%s dim=%d)", self.model_name, actual_dim)
            return self._model

    # ------------------------------------------------------------------
    def encode(
        self,
        texts: Iterable[str],
        batch_size: int = DEFAULT_BATCH,
        show_progress: bool = False,
    ) -> np.ndarray:
        """Return a ``(n, dim)`` float32 array of L2-normalised vectors."""
        items = list(texts)
        if not items:
            return np.zeros((0, self.expected_dim), dtype=np.float32)

        model = self._ensure_model()
        try:
            vec = model.encode(
                items,
                batch_size=batch_size,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=show_progress,
            )
        except Exception as exc:
            raise EmbeddingError(f"Encoding failed: {exc}") from exc

        if vec.ndim != 2 or vec.shape[1] != self.expected_dim:
            raise EmbeddingError(
                f"Unexpected embedding shape {vec.shape}; expected (*, {self.expected_dim})"
            )
        # FAISS wants contiguous float32.
        return np.ascontiguousarray(vec, dtype=np.float32)

    def encode_one(self, text: str) -> np.ndarray:
        """Return a ``(1, dim)`` array for a single query string."""
        return self.encode([text or ""], batch_size=1)


# Module-level singleton reused by both the FastAPI lifespan and the
# build script. Keep this small — it's just a model handle.
_default: Embedder | None = None
_default_lock = threading.Lock()


def get_default_embedder() -> Embedder:
    global _default
    if _default is not None:
        return _default
    with _default_lock:
        if _default is None:
            _default = Embedder()
    return _default
