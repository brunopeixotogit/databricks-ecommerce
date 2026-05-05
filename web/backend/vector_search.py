"""Thin wrapper around Databricks Vector Search.

We hold a single ``VectorSearchClient`` and look up the index lazily
(the underlying object caches its endpoint/auth, so repeated lookups are
cheap). All callers expect ``ProductHit`` rows or an empty list — failures
are logged and surfaced as ``VectorSearchUnavailable`` so the catalog
layer can fall back to SQL without leaking SDK exceptions to FastAPI.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Iterable

logger = logging.getLogger(__name__)


class VectorSearchUnavailable(RuntimeError):
    """The index is missing, the endpoint is down, or auth failed."""


@dataclass(frozen=True)
class ProductHit:
    product_id: str
    name: str
    category: str
    price: float
    currency: str
    brand: str | None
    score: float

    def to_dict(self) -> dict[str, object]:
        return {
            "product_id": self.product_id,
            "name": self.name,
            "category": self.category,
            "price": self.price,
            "currency": self.currency,
            "brand": self.brand,
            "score": self.score,
        }


# Columns we ask the index to return on every query. Must exist on the
# source table (see pipelines/vector_search/setup.py).
_RETURN_COLS = ["product_id", "name", "category", "price", "currency", "brand"]


class ProductVectorIndex:
    """Semantic search over ``products_vs_index``.

    Configuration via environment:
        VS_ENDPOINT           — Vector Search endpoint name
        VS_INDEX_NAME         — Fully-qualified UC index name
    """

    def __init__(
        self,
        endpoint_name: str | None = None,
        index_name: str | None = None,
    ) -> None:
        self.endpoint_name = endpoint_name or os.getenv("VS_ENDPOINT", "bricksshop_vs")
        self.index_name = index_name or os.getenv(
            "VS_INDEX_NAME", "dev_main.ecom_dlt.products_vs_index"
        )
        self._index = None  # lazy
        self._unavailable: str | None = None  # cached reason once we've failed

    # ------------------------------------------------------------------
    def search(
        self,
        query_text: str,
        *,
        category: str | None = None,
        max_price: float | None = None,
        num_results: int = 5,
    ) -> list[ProductHit]:
        """Run a semantic similarity search and return up to ``num_results`` hits."""
        if not query_text or not query_text.strip():
            return []
        if self._unavailable:
            raise VectorSearchUnavailable(self._unavailable)

        idx = self._get_index()
        filters: dict[str, object] = {}
        if category:
            filters["category"] = category
        if max_price is not None and max_price > 0:
            # Vector Search filter syntax for inclusive upper bound.
            filters["price <="] = float(max_price)

        try:
            resp = idx.similarity_search(
                query_text=query_text,
                columns=_RETURN_COLS,
                num_results=max(1, min(int(num_results), 20)),
                filters=filters or None,
            )
        except Exception as exc:
            self._mark_unavailable(f"similarity_search failed: {exc}")
            raise VectorSearchUnavailable(str(exc)) from exc

        return list(_iter_hits(resp))

    # ------------------------------------------------------------------
    def _get_index(self):
        if self._index is not None:
            return self._index
        try:
            from databricks.vector_search.client import VectorSearchClient
        except ImportError as exc:
            self._mark_unavailable(
                "databricks-vectorsearch not installed (add to requirements.txt)"
            )
            raise VectorSearchUnavailable(str(exc)) from exc

        try:
            client = VectorSearchClient(disable_notice=True)
            self._index = client.get_index(
                endpoint_name=self.endpoint_name,
                index_name=self.index_name,
            )
        except Exception as exc:
            self._mark_unavailable(f"get_index failed: {exc}")
            raise VectorSearchUnavailable(str(exc)) from exc

        logger.info(
            "Vector Search ready: endpoint=%s index=%s",
            self.endpoint_name, self.index_name,
        )
        return self._index

    def _mark_unavailable(self, reason: str) -> None:
        if self._unavailable is None:
            logger.warning("Vector Search unavailable: %s", reason)
        self._unavailable = reason


# ---------------------------------------------------------------------------
# Response parsing — the SDK returns a dict shaped like
#   {"manifest": {"columns":[{"name":..,"type":..}, ...]},
#    "result": {"data_array": [[v1, v2, ..., score], ...], "row_count": N}}
# Rows include the score as the LAST element regardless of requested cols.
# ---------------------------------------------------------------------------
def _iter_hits(resp: dict) -> Iterable[ProductHit]:
    if not isinstance(resp, dict):
        return
    cols = [c["name"] for c in resp.get("manifest", {}).get("columns", [])]
    rows = resp.get("result", {}).get("data_array", []) or []
    for row in rows:
        try:
            d = dict(zip(cols, row))
            yield ProductHit(
                product_id=str(d.get("product_id", "")),
                name=str(d.get("name", "")),
                category=str(d.get("category", "")),
                price=float(d.get("price") or 0.0),
                currency=str(d.get("currency") or "USD"),
                brand=(str(d["brand"]) if d.get("brand") is not None else None),
                # The score column is named "__score__" in the manifest.
                score=float(d.get("__score__") or d.get("score") or 0.0),
            )
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping malformed VS row %r: %s", row, exc)
            continue
