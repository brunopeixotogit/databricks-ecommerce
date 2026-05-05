"""Delta-backed product retrieval.

All reads go to ``dev_main.ecom_dlt.dim_products_scd2`` filtered to
``is_current AND active``. We use the Statement Execution API via the
Databricks SDK so the warehouse handles concurrency and we don't need a
JDBC/ODBC driver.

Queries are parameterised — no string interpolation of user input — to
keep the chat endpoint safe from injection.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from .faiss_index import FaissHit, FaissProductIndex, FaissUnavailable
from .ranking import HybridRanker
from .vector_search import ProductHit, ProductVectorIndex, VectorSearchUnavailable

logger = logging.getLogger(__name__)

VALID_CATEGORIES = ("electronics", "appliances", "furniture")
PRODUCT_TABLE = os.getenv("PRODUCT_TABLE", "dev_main.ecom_dlt.dim_products_scd2")


class ProductLookupError(RuntimeError):
    """SQL execution failed or returned an unexpected shape."""


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    price: float
    currency: str
    # Semantic similarity from the retrieval tier (VS / FAISS). None for
    # SQL-fallback rows, which never enter the hybrid ranker.
    score: float | None = None
    # Components produced by HybridRanker, populated only on the
    # semantic tiers. None on the SQL fallback path (or when ranking
    # is disabled). Surfacing these on the wire is optional — the chat
    # layer maps Product to a leaner ChatProduct — but they're useful
    # for /admin debug endpoints and for unit tests of the ranker.
    price_score: float | None = None
    popularity_score: float | None = None
    final_score: float | None = None

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {
            "product_id": self.product_id,
            "name": self.name,
            "category": self.category,
            "price": self.price,
            "currency": self.currency,
        }
        if self.score is not None:
            d["score"] = self.score
        if self.price_score is not None:
            d["price_score"] = self.price_score
        if self.popularity_score is not None:
            d["popularity_score"] = self.popularity_score
        if self.final_score is not None:
            d["final_score"] = self.final_score
        return d


class ProductCatalog:
    """Thin wrapper around the SQL warehouse for product reads."""

    def __init__(
        self,
        warehouse_id: str | None = None,
        table: str | None = None,
        workspace: WorkspaceClient | None = None,
        vector_index: ProductVectorIndex | None = None,
        faiss_index: FaissProductIndex | None = None,
        ranker: HybridRanker | None = None,
    ) -> None:
        self.warehouse_id = warehouse_id or os.getenv("SQL_WAREHOUSE_ID", "")
        self.table = table or PRODUCT_TABLE
        if not self.warehouse_id:
            raise ProductLookupError(
                "SQL_WAREHOUSE_ID is required for product lookups."
            )
        self._w = workspace or WorkspaceClient()
        # Both semantic backends are constructed lazily on first call so
        # a missing index never blocks startup. Tests can inject mocks.
        self._vector = vector_index if vector_index is not None else ProductVectorIndex()
        # FAISS is wired in lazily by the FastAPI lifespan once the
        # local model + Volume pointer are usable. Catalogs constructed
        # outside the app (tests, scripts) just won't have it, and the
        # fallback chain will skip that tier.
        self._faiss = faiss_index
        # Hybrid ranker — applied AFTER retrieval. None means "skip the
        # re-ranking step entirely and just return semantic order",
        # which is the correct behaviour when the backend is starting
        # up and the popularity cache hasn't filled yet.
        self._ranker = ranker

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def attach_faiss(self, faiss_index: FaissProductIndex | None) -> None:
        """Wire the FAISS tier in after construction (used by lifespan)."""
        self._faiss = faiss_index

    def attach_ranker(self, ranker: HybridRanker | None) -> None:
        """Wire the hybrid ranker in after construction (used by lifespan)."""
        self._ranker = ranker

    # Oversample factor used for the retrieval ⇒ rank handoff. We pull
    # `limit * RERANK_OVERSAMPLE` candidates from the retrieval tier so
    # the ranker has room to reorder; then we trim back to ``limit``.
    # 4× is a sweet spot between ranker headroom and warehouse cost.
    RERANK_OVERSAMPLE = 4

    def semantic_search(
        self,
        query: str,
        category: str | None = None,
        max_price: float | None = None,
        limit: int = 5,
    ) -> list[Product]:
        """Three-tier semantic lookup, with hybrid re-ranking on top:

            1. Databricks Vector Search (production tier)  — re-ranked
            2. FAISS index in a Volume         (fallback)  — re-ranked
            3. SQL ILIKE on dim_products_scd2  (last resort) — not ranked

        Each tier is tried in turn; we move on as soon as one tier is
        unavailable OR returns zero hits. The retrieval step is
        unchanged from before — the ranker only re-orders the
        candidates the retrieval already produced (``limit *
        RERANK_OVERSAMPLE`` of them), then we trim back to ``limit``.

        ``Product.score`` is populated for tiers 1 and 2; when the
        ranker is attached, ``price_score`` / ``popularity_score`` /
        ``final_score`` are populated too. Tier 3 returns rows with
        all four set to ``None`` — no semantic axis exists to combine
        with.
        """
        if not query or not query.strip():
            return self.search(category=category, max_price=max_price, limit=limit)

        candidate_pool = max(limit, limit * self.RERANK_OVERSAMPLE) if self._ranker else limit

        # ---- Tier 1: Databricks Vector Search ----------------------------
        try:
            hits = self._vector.search(
                query_text=query,
                category=category,
                max_price=max_price,
                num_results=candidate_pool,
            )
            if hits:
                products = [_vs_hit_to_product(h) for h in hits]
                ranked = self._apply_ranker(products, limit=limit, tier="VS", query=query)
                return ranked
            logger.info("semantic[VS]: 0 hits for %r — trying FAISS", query)
        except VectorSearchUnavailable as exc:
            logger.info("semantic[VS]: unavailable (%s) — trying FAISS", exc)

        # ---- Tier 2: FAISS index in a Volume -----------------------------
        if self._faiss is not None and self._faiss.is_ready():
            try:
                fhits = self._faiss.search(query_text=query, num_results=candidate_pool)
                products = self._materialise_faiss_hits(
                    fhits, category=category, max_price=max_price,
                )
                if products:
                    ranked = self._apply_ranker(products, limit=limit, tier="FAISS", query=query)
                    return ranked
                logger.info("semantic[FAISS]: 0 surviving hits — trying SQL")
            except FaissUnavailable as exc:
                logger.info("semantic[FAISS]: unavailable (%s) — trying SQL", exc)
            except Exception as exc:
                logger.exception("semantic[FAISS]: unexpected error %s — trying SQL", exc)

        # ---- Tier 3: SQL fallback ----------------------------------------
        return self.search(category=category, query=query, max_price=max_price, limit=limit)

    def _apply_ranker(
        self,
        products: list[Product],
        *,
        limit: int,
        tier: str,
        query: str,
    ) -> list[Product]:
        """Run the hybrid ranker (if attached) and trim to ``limit``.

        When the ranker is not attached we fall back to the retrieval-
        order semantics: keep semantic order, trim to ``limit``. This
        is what callers get during startup before the popularity cache
        has filled, and also what tests get when they construct a
        ``ProductCatalog`` without a ranker.
        """
        if not products:
            return []
        if self._ranker is None:
            top = products[:limit]
            logger.info(
                "semantic[%s]: %d hits for %r (no ranker, top score %.3f)",
                tier, len(top), query, top[0].score or 0.0,
            )
            return top
        ranked = self._ranker.rank(products, limit=limit)
        top = ranked[0] if ranked else None
        if top is not None:
            logger.info(
                "semantic[%s]: %d/%d ranked for %r — top final=%.3f "
                "(sem=%.3f price=%.3f pop=%.3f) pid=%s",
                tier, len(ranked), len(products), query,
                top.final_score or 0.0,
                top.score or 0.0,
                top.price_score or 0.0,
                top.popularity_score or 0.0,
                top.product_id,
            )
        return ranked

    def _materialise_faiss_hits(
        self,
        hits: list[FaissHit],
        *,
        category: str | None,
        max_price: float | None,
    ) -> list[Product]:
        """Join FAISS hits to current/active product rows; preserve
        FAISS-score order.

        FAISS only knows ``product_id`` and ``score``. We need the
        current name/price/category, plus the category and price filters
        the user asked for. One SQL round-trip retrieves all of it.

        No trimming here — the caller (``semantic_search``) owns the
        trimming policy: with a ranker attached, we want the full
        candidate pool to flow through the ranker; without one, the
        caller trims to limit by retrieval order.
        """
        if not hits:
            return []
        score_by_id = {h.product_id: h.score for h in hits}
        ids = list(score_by_id)

        clauses = ["is_current = TRUE", "active = TRUE", "product_id IN (:ids)"]
        # No native ``IN`` parameter binding in Statement Execution, so
        # quote-and-join after validating each id is a safe slug.
        # product_id values are UUIDs / hex strings in this catalog.
        safe_ids = [pid for pid in ids if pid and all(c.isalnum() or c == "_" or c == "-" for c in pid)]
        if not safe_ids:
            return []
        ids_sql = ", ".join(f"'{pid}'" for pid in safe_ids)
        clauses[-1] = f"product_id IN ({ids_sql})"

        params: list[StatementParameterListItem] = []
        if category and category in VALID_CATEGORIES:
            clauses.append("category = :category")
            params.append(StatementParameterListItem(name="category", value=category))
        if max_price is not None and max_price > 0:
            clauses.append("price <= :max_price")
            params.append(
                StatementParameterListItem(
                    name="max_price", value=str(float(max_price)), type="DOUBLE"
                )
            )

        sql = (
            f"SELECT product_id, name, category, price, currency "
            f"FROM {self.table} "
            f"WHERE {' AND '.join(clauses)}"
        )
        rows = self._exec(sql, params)

        products: list[Product] = []
        for r in rows:
            pid = r[0]
            products.append(Product(
                product_id=pid,
                name=r[1],
                category=r[2],
                price=float(r[3]) if r[3] is not None else 0.0,
                currency=r[4] or "USD",
                score=score_by_id.get(pid),
            ))
        # Sort by FAISS score (descending) so the ranker — and the
        # no-ranker fallback — see candidates in retrieval order.
        products.sort(key=lambda p: p.score if p.score is not None else -1.0, reverse=True)
        return products

    def search(
        self,
        category: str | None = None,
        query: str | None = None,
        max_price: float | None = None,
        limit: int = 5,
    ) -> list[Product]:
        """Search current/active products with optional filters.

        ``query`` matches against ``name`` (ILIKE). ``category`` is
        validated against the known set so we can hand it to the SQL
        without escaping.
        """
        limit = max(1, min(int(limit), 25))
        clauses = ["is_current = TRUE", "active = TRUE"]
        params: list[StatementParameterListItem] = []

        if category and category in VALID_CATEGORIES:
            clauses.append("category = :category")
            params.append(StatementParameterListItem(name="category", value=category))

        if query:
            clauses.append("LOWER(name) LIKE :query")
            params.append(
                StatementParameterListItem(name="query", value=f"%{query.lower()}%")
            )

        if max_price is not None and max_price > 0:
            clauses.append("price <= :max_price")
            params.append(
                StatementParameterListItem(
                    name="max_price", value=str(float(max_price)), type="DOUBLE"
                )
            )

        where = " AND ".join(clauses)
        sql = (
            f"SELECT product_id, name, category, price, currency "
            f"FROM {self.table} "
            f"WHERE {where} "
            f"ORDER BY price ASC "
            f"LIMIT {limit}"
        )
        rows = self._exec(sql, params)
        return [self._row_to_product(r) for r in rows]

    def get(self, product_id: str) -> Product | None:
        """Fetch a single product by id (current version)."""
        if not product_id:
            return None
        sql = (
            f"SELECT product_id, name, category, price, currency "
            f"FROM {self.table} "
            f"WHERE product_id = :pid AND is_current = TRUE "
            f"LIMIT 1"
        )
        rows = self._exec(
            sql, [StatementParameterListItem(name="pid", value=product_id)]
        )
        if not rows:
            return None
        return self._row_to_product(rows[0])

    def categories(self) -> tuple[str, ...]:
        return VALID_CATEGORIES

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _exec(
        self, sql: str, params: Iterable[StatementParameterListItem]
    ) -> list[list[str]]:
        try:
            r = self._w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                parameters=list(params),
                wait_timeout="30s",
            )
            sid = r.statement_id
            # Poll briefly if the warehouse was cold.
            deadline = time.monotonic() + 25.0
            while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
                if time.monotonic() > deadline:
                    raise ProductLookupError(
                        f"SQL timed out after 25s (statement_id={sid})"
                    )
                time.sleep(1.5)
                r = self._w.statement_execution.get_statement(sid)

            if r.status.state != StatementState.SUCCEEDED:
                msg = r.status.error.message if r.status.error else r.status.state
                raise ProductLookupError(f"SQL failed: {msg}")
        except ProductLookupError:
            raise
        except Exception as exc:  # SDK raises a variety of types
            raise ProductLookupError(f"SQL exec error: {exc}") from exc

        if not r.result or not r.result.data_array:
            return []
        return r.result.data_array

    @staticmethod
    def _row_to_product(row: list[str]) -> Product:
        return Product(
            product_id=row[0],
            name=row[1],
            category=row[2],
            price=float(row[3]) if row[3] is not None else 0.0,
            currency=row[4] or "USD",
        )


def _vs_hit_to_product(hit: ProductHit) -> Product:
    return Product(
        product_id=hit.product_id,
        name=hit.name,
        category=hit.category,
        price=hit.price,
        currency=hit.currency,
        score=hit.score,
    )
