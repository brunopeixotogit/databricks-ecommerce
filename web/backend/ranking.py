"""Hybrid ranking layer applied AFTER semantic retrieval.

The retrieval tiers (Vector Search, FAISS) return products ranked by
semantic similarity alone. That's a good starting point, but it ignores
two things shoppers actually care about:

* **Price.** Two equally-relevant products at $40 and $400 should not
  rank the same.
* **Popularity.** A semantically-perfect match nobody has bought in a
  week is probably less useful than a slightly-less-perfect match that's
  flying off the shelf.

This module produces:

    final_score = w_s * semantic_similarity
                + w_p * price_score
                + w_pop * popularity_score

All three components are normalised into ``[0, 1]`` before the weighted
sum, so the weights are interpretable. Defaults — see
``HybridRanker.DEFAULT_WEIGHTS`` — favour semantic relevance with a
material popularity contribution and a price tie-breaker.

Two cooperating classes live here:

* :class:`PopularitySignals` — periodically aggregates 7-day product
  activity from the silver/gold Delta tables, caches the result in
  memory, and exposes ``popularity_score(product_id)``. A daemon thread
  refreshes the cache every ``POPULARITY_REFRESH_INTERVAL_S`` seconds
  (default 15 min) so the chat path never blocks on SQL.

* :class:`HybridRanker` — pure function-style helper that takes the
  retrieved products, asks ``PopularitySignals`` for the popularity
  component, computes the price component per-query (min-max within
  the candidate set), and returns the products re-sorted by
  ``final_score``.

The ranker is wired into ``ProductCatalog`` and applied to both the
Vector Search and FAISS tiers. The SQL fallback tier returns
unscored rows; the ranker leaves it alone (no semantic component to
combine with).
"""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from .products import Product

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class PopularityUnavailable(RuntimeError):
    """Cache hasn't been populated yet (initial refresh failed and no
    prior snapshot exists). Callers should treat popularity as 0.0 and
    keep going — never block a chat reply on a popularity miss."""


# ---------------------------------------------------------------------------
# Per-product activity row + global maxima used for normalisation.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class _ProductActivity:
    purchases_7d: int
    add_to_cart_7d: int
    views_7d: int


@dataclass(frozen=True)
class _PopularitySnapshot:
    """Immutable snapshot the poller swaps in atomically."""
    rows: dict[str, _ProductActivity]
    max_purchases: int
    max_add_to_cart: int
    max_views: int
    refreshed_at: float          # monotonic seconds


# ---------------------------------------------------------------------------
# PopularitySignals — polled cache over fact_orders + events.
# ---------------------------------------------------------------------------
class PopularitySignals:
    """Polled in-memory cache of recent product activity.

    Configuration via environment:
        SQL_WAREHOUSE_ID                  — required
        EVENTS_TABLE                      — default: dev_main.ecom_dlt.events
        ORDERS_TABLE                      — default: dev_main.ecom_dlt.fact_orders
        POPULARITY_WINDOW_DAYS            — default: 7
        POPULARITY_REFRESH_INTERVAL_S     — default: 900 (15 min)

    Refresh strategy: a single SQL roundtrip combines event- and order-
    derived signals via ``FULL OUTER JOIN``. We pull aggregates only
    (one row per product), never raw event rows, so the result is small
    enough to hold in memory comfortably even for catalogs in the
    hundreds-of-thousands range.
    """

    def __init__(
        self,
        warehouse_id: str | None = None,
        events_table: str | None = None,
        orders_table: str | None = None,
        window_days: int | None = None,
        refresh_interval_s: float | None = None,
    ) -> None:
        self.warehouse_id = warehouse_id or os.getenv("SQL_WAREHOUSE_ID", "")
        self.events_table = events_table or os.getenv(
            "EVENTS_TABLE", "dev_main.ecom_dlt.events"
        )
        self.orders_table = orders_table or os.getenv(
            "ORDERS_TABLE", "dev_main.ecom_dlt.fact_orders"
        )
        self.window_days = int(window_days or os.getenv("POPULARITY_WINDOW_DAYS", "7"))
        self.refresh_interval_s = float(
            refresh_interval_s or os.getenv("POPULARITY_REFRESH_INTERVAL_S", "900")
        )

        if not self.warehouse_id:
            raise PopularityUnavailable("SQL_WAREHOUSE_ID required for popularity signals")

        self._snapshot: _PopularitySnapshot | None = None
        self._swap_lock = threading.Lock()
        self._stop = threading.Event()
        self._poll_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Run an initial refresh and start the background poller.

        Like the FAISS loader, both initial load and the poller are
        best-effort: if the warehouse is unreachable or the tables are
        empty we leave the snapshot empty and let ``popularity_score``
        return 0.0 for everything. The poller keeps retrying.
        """
        try:
            self.refresh()
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning("PopularitySignals: initial refresh failed: %s", exc)

        if self._poll_thread is not None:
            return
        self._stop.clear()
        t = threading.Thread(
            target=self._poll_loop, name="popularity-poller", daemon=True
        )
        t.start()
        self._poll_thread = t
        logger.info(
            "PopularitySignals poller started (every %.0fs, window=%dd)",
            self.refresh_interval_s, self.window_days,
        )

    def close(self) -> None:
        self._stop.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2.0)
            self._poll_thread = None

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------
    def is_ready(self) -> bool:
        return self._snapshot is not None

    def info(self) -> dict[str, object]:
        snap = self._snapshot
        if snap is None:
            return {"ready": False, "window_days": self.window_days}
        return {
            "ready":           True,
            "n_products":      len(snap.rows),
            "max_purchases":   snap.max_purchases,
            "max_add_to_cart": snap.max_add_to_cart,
            "max_views":       snap.max_views,
            "window_days":     self.window_days,
            "refreshed_s_ago": round(time.monotonic() - snap.refreshed_at, 1),
        }

    def get(self, product_id: str) -> _ProductActivity:
        snap = self._snapshot
        if snap is None:
            return _ProductActivity(0, 0, 0)
        return snap.rows.get(product_id, _ProductActivity(0, 0, 0))

    def popularity_score(self, product_id: str) -> float:
        """Return a popularity score in ``[0, 1]`` for ``product_id``.

        Combines three signals on a log scale (so the head doesn't
        dominate) and globally normalises against the catalog max. The
        per-signal weights inside the popularity component encode
        intent strength: ``purchase >> add_to_cart >> view``.
        """
        snap = self._snapshot
        if snap is None:
            return 0.0
        a = snap.rows.get(product_id)
        if a is None:
            return 0.0

        # log1p softens the long tail: a product with 100 purchases
        # shouldn't dwarf one with 50 by a factor of 2 once normalised.
        p_norm = _safe_log_norm(a.purchases_7d,  snap.max_purchases)
        c_norm = _safe_log_norm(a.add_to_cart_7d, snap.max_add_to_cart)
        v_norm = _safe_log_norm(a.views_7d,       snap.max_views)

        # Internal weights inside popularity — strongest intent wins.
        score = 0.6 * p_norm + 0.3 * c_norm + 0.1 * v_norm
        return _clip01(score)

    # ------------------------------------------------------------------
    # Refresh mechanics
    # ------------------------------------------------------------------
    def refresh(self) -> int:
        """Re-aggregate from Delta and atomically swap the snapshot.

        Returns the number of product rows in the new snapshot.
        """
        # Local import keeps databricks-sdk out of the cold-import path
        # for code that just imports the ranker for typing.
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        w = WorkspaceClient()
        sql = self._build_sql()
        r = w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=sql, wait_timeout="30s"
        )
        sid = r.statement_id
        deadline = time.monotonic() + 60.0
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                raise PopularityUnavailable(f"popularity SQL timed out: {sid}")
            time.sleep(1.5)
            r = w.statement_execution.get_statement(sid)
        if r.status.state != StatementState.SUCCEEDED:
            msg = r.status.error.message if r.status.error else r.status.state
            raise PopularityUnavailable(f"popularity SQL failed: {msg}")

        rows_raw = (r.result.data_array if r.result else None) or []
        rows: dict[str, _ProductActivity] = {}
        max_p = max_c = max_v = 0
        for row in rows_raw:
            try:
                pid = row[0]
                if not pid:
                    continue
                purchases = int(row[1] or 0)
                add_to_cart = int(row[2] or 0)
                views = int(row[3] or 0)
            except (TypeError, ValueError, IndexError) as exc:
                logger.warning("Skipping malformed popularity row %r: %s", row, exc)
                continue
            rows[pid] = _ProductActivity(purchases, add_to_cart, views)
            if purchases  > max_p: max_p = purchases
            if add_to_cart > max_c: max_c = add_to_cart
            if views      > max_v: max_v = views

        snap = _PopularitySnapshot(
            rows=rows,
            max_purchases=max_p,
            max_add_to_cart=max_c,
            max_views=max_v,
            refreshed_at=time.monotonic(),
        )
        with self._swap_lock:
            self._snapshot = snap
        logger.info(
            "PopularitySignals refreshed: %d products (max p=%d c=%d v=%d)",
            len(rows), max_p, max_c, max_v,
        )
        return len(rows)

    # ------------------------------------------------------------------
    def _build_sql(self) -> str:
        """The single SQL roundtrip that backs ``refresh()``.

        Uses both ``events`` (for views + adds) and ``fact_orders``
        (for the canonical purchase counts, since fact_orders is the
        deduplicated authoritative purchase grain). Results are joined
        ``FULL OUTER`` so a product that's only viewed but never bought
        — or vice versa — still produces a row.
        """
        return f"""
        WITH event_signals AS (
            SELECT
                product_id,
                SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_7d,
                SUM(CASE WHEN event_type = 'page_view'   THEN 1 ELSE 0 END) AS views_7d
            FROM {self.events_table}
            WHERE event_ts >= current_timestamp() - INTERVAL {self.window_days} DAYS
              AND product_id IS NOT NULL
              AND event_type IN ('page_view', 'add_to_cart')
            GROUP BY product_id
        ),
        order_signals AS (
            SELECT
                item.product_id AS product_id,
                CAST(SUM(item.quantity) AS BIGINT) AS purchases_7d
            FROM {self.orders_table}
            LATERAL VIEW EXPLODE(items) t AS item
            WHERE order_ts >= current_timestamp() - INTERVAL {self.window_days} DAYS
              AND item.product_id IS NOT NULL
            GROUP BY item.product_id
        )
        SELECT
            COALESCE(e.product_id, o.product_id) AS product_id,
            COALESCE(o.purchases_7d, 0)          AS purchases_7d,
            COALESCE(e.add_to_cart_7d, 0)        AS add_to_cart_7d,
            COALESCE(e.views_7d, 0)              AS views_7d
        FROM event_signals e
        FULL OUTER JOIN order_signals o
          ON e.product_id = o.product_id
        WHERE COALESCE(e.product_id, o.product_id) IS NOT NULL
        """

    def _poll_loop(self) -> None:
        last_err: str | None = None
        while not self._stop.is_set():
            self._stop.wait(self.refresh_interval_s)
            if self._stop.is_set():
                break
            try:
                self.refresh()
                last_err = None
            except Exception as exc:  # noqa: BLE001 — never let the thread die
                msg = str(exc)
                if msg != last_err:
                    logger.warning("PopularitySignals poll: %s", msg)
                    last_err = msg


# ---------------------------------------------------------------------------
# HybridRanker — the actual re-scoring step.
# ---------------------------------------------------------------------------
class HybridRanker:
    """Re-rank semantic-search candidates with a hybrid score.

    All three signals — semantic similarity, inverse price, popularity —
    are normalised into ``[0, 1]`` so the user-supplied weights are
    interpretable as a relative emphasis. Weights below sum to 1.0 by
    default; the ranker doesn't enforce that because some operators
    deliberately under-weight (e.g. weights summing to 0.9 leaves
    headroom for a future signal).
    """

    DEFAULT_WEIGHTS = {
        "semantic":   0.60,
        "price":      0.15,
        "popularity": 0.25,
    }

    def __init__(
        self,
        popularity: PopularitySignals | None = None,
        w_semantic: float | None = None,
        w_price: float | None = None,
        w_popularity: float | None = None,
    ) -> None:
        self.popularity = popularity
        self.w_semantic   = _read_weight("SEMANTIC_WEIGHT",   w_semantic,   self.DEFAULT_WEIGHTS["semantic"])
        self.w_price      = _read_weight("PRICE_WEIGHT",      w_price,      self.DEFAULT_WEIGHTS["price"])
        self.w_popularity = _read_weight("POPULARITY_WEIGHT", w_popularity, self.DEFAULT_WEIGHTS["popularity"])
        logger.info(
            "HybridRanker weights: semantic=%.2f price=%.2f popularity=%.2f",
            self.w_semantic, self.w_price, self.w_popularity,
        )

    # ------------------------------------------------------------------
    def weights(self) -> dict[str, float]:
        return {
            "semantic":   self.w_semantic,
            "price":      self.w_price,
            "popularity": self.w_popularity,
        }

    def rank(self, products: Sequence["Product"], *, limit: int | None = None) -> list["Product"]:
        """Score, sort, and (optionally) trim the input list.

        The input is expected to be the materialised output of a
        semantic retrieval tier — every product carries a ``score``
        (semantic similarity). Products that lack a score are scored
        as 0.0 on the semantic axis, which is correct for SQL-fallback
        rows but rare on the tiers this ranker actually runs against.

        The original :class:`Product` is replaced by a copy carrying
        the per-component scores plus ``final_score``, so the chat
        layer (or a debug endpoint) can surface the breakdown.
        """
        if not products:
            return []

        # Per-query price normalisation. Using the candidate-set's own
        # min/max keeps the price signal meaningful regardless of the
        # absolute price band of the query (a $50 toaster query and a
        # $5000 sofa query both get a full-range price contribution).
        prices = [_safe_price(p.price) for p in products]
        p_min, p_max = min(prices), max(prices)
        p_span = p_max - p_min

        ranked: list[Product] = []
        for p, price_val in zip(products, prices):
            sem  = _clip01(p.score if p.score is not None else 0.0)
            pric = 1.0 if p_span == 0 else (p_max - price_val) / p_span
            pop  = self.popularity.popularity_score(p.product_id) if self.popularity is not None else 0.0

            final = self.w_semantic * sem + self.w_price * pric + self.w_popularity * pop
            ranked.append(_with_scores(p, sem, pric, pop, final))

        ranked.sort(key=lambda r: r.final_score or 0.0, reverse=True)
        if limit is not None:
            ranked = ranked[: max(1, int(limit))]
        return ranked


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------
def _clip01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    if x < 0.0: return 0.0
    if x > 1.0: return 1.0
    return float(x)


def _safe_log_norm(value: int, ceiling: int) -> float:
    """log1p-normalised against a non-zero ceiling. Returns 0.0 when
    ceiling is 0 (no activity in the window — defines popularity as 0
    for everyone, which is the desired neutral)."""
    if ceiling <= 0:
        return 0.0
    return math.log1p(max(0, value)) / math.log1p(ceiling)


def _safe_price(price: float | None) -> float:
    """Map missing / non-positive prices to 0.0 for the price signal.

    A free product reads as the cheapest possible (price_score=1.0)
    after min-max normalisation, which is the correct user-facing
    behaviour."""
    if price is None or price < 0 or price != price:
        return 0.0
    return float(price)


def _read_weight(env_name: str, override: float | None, default: float) -> float:
    if override is not None:
        return float(override)
    raw = os.getenv(env_name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "Invalid %s=%r; falling back to default %.2f", env_name, raw, default
        )
        return float(default)


def _with_scores(
    p: "Product",
    semantic: float,
    price: float,
    popularity: float,
    final: float,
) -> "Product":
    """Return a copy of ``p`` with the four score components set.

    ``Product`` is a frozen dataclass, so we use ``dataclasses.replace``
    to build the copy; that also keeps any future fields we add to
    :class:`Product` automatically copied through.
    """
    from dataclasses import replace
    return replace(
        p,
        score=semantic,
        price_score=price,
        popularity_score=popularity,
        final_score=final,
    )
