# Hybrid ranking

Layer that runs **after** semantic retrieval and **before** the chat
reply, re-scoring the candidate products to balance relevance, price,
and recent popularity. Retrieval is unchanged; this layer only
re-orders what the retrieval already produced.

```
Vector Search / FAISS  ──►  HybridRanker  ──►  /chat reply
   (top-K candidates)        (re-scored,         (top-N shown to user)
                              top-N kept)
```

---

## 1 · The score

```
final_score = w_s   * semantic_similarity
            + w_p   * price_score
            + w_pop * popularity_score
```

All three components are normalised into `[0, 1]` before the weighted
sum, so the weights are a direct expression of relative emphasis.

| Component | Source | Range |
|---|---|---|
| `semantic_similarity` | Cosine score from VS / FAISS | `[0, 1]` (clipped) |
| `price_score` | Inverse price, min-max within candidate set | `[0, 1]` |
| `popularity_score` | log-norm of 7-day events + orders | `[0, 1]` |

Defaults — see `ranking.py` `HybridRanker.DEFAULT_WEIGHTS`:

```
SEMANTIC_WEIGHT   = 0.60
PRICE_WEIGHT      = 0.15
POPULARITY_WEIGHT = 0.25
```

Override per environment via env vars (same names, uppercase). The
ranker accepts any non-negative floats — they don't have to sum to 1.0,
though defaults do for interpretability.

---

## 2 · Signals

### 2.1 — Semantic similarity

Pulled straight from the retrieval tier:

* **Vector Search** returns a similarity score per row; we take it as-is.
* **FAISS** uses `IndexFlatIP` over L2-normalised vectors → cosine
  similarity, typically in `[0, 1]` for relevant matches.

The ranker clips into `[0, 1]` defensively (occasionally a near-zero
match can produce a tiny negative).

### 2.2 — Price score

Per-query min-max over the candidate set:

```
price_score(p) = (max_price_in_pool - p.price) / (max_price - min_price)
```

* Cheapest candidate → `1.0`.
* Most expensive → `0.0`.
* Single-price candidate set (e.g. all $99) → all `1.0`. The ranker
  refuses to discriminate when there's nothing to discriminate on.

We deliberately use the candidate-set's own min/max rather than a
catalog-wide global. This way the price signal carries equal weight
whether the user is shopping for $20 toasters or $5,000 sofas — it
always spans the full `[0, 1]` band relative to the alternatives the
user is actually choosing between.

### 2.3 — Popularity score

Aggregated from two existing Delta tables over a rolling window
(default 7 days):

* `purchases_7d` ← `dev_main.ecom_dlt.fact_orders` (exploded `items`
  array, summed `quantity`)
* `add_to_cart_7d` ← `dev_main.ecom_dlt.events` filtered to
  `event_type = 'add_to_cart'`
* `views_7d` ← `dev_main.ecom_dlt.events` filtered to
  `event_type = 'page_view'`

These three are combined with internal weights that encode intent
strength (purchase ≫ cart ≫ view), each individually log-normalised
against the catalog max for that signal:

```python
def popularity_score(pid):
    a = signals[pid]                                # 0 if missing
    p = log1p(a.purchases_7d)   / log1p(max_purchases)
    c = log1p(a.add_to_cart_7d) / log1p(max_add_to_cart)
    v = log1p(a.views_7d)       / log1p(max_views)
    return 0.6 * p + 0.3 * c + 0.1 * v          # all in [0, 1]
```

`log1p` softens the long tail so that a runaway top-seller doesn't
flatten everything else to ~0.

The cache is refreshed in a background thread every
`POPULARITY_REFRESH_INTERVAL_S` seconds (default 900). Chat replies
never block on the popularity SQL — when the cache is cold or the
warehouse is unreachable, the popularity component reads as `0.0` for
everyone and the score reduces to `w_s * semantic + w_p * price`.

### 2.4 — Aggregation SQL

Single roundtrip, both tables joined with `FULL OUTER JOIN`:

```sql
WITH event_signals AS (
  SELECT
    product_id,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_7d,
    SUM(CASE WHEN event_type = 'page_view'   THEN 1 ELSE 0 END) AS views_7d
  FROM dev_main.ecom_dlt.events
  WHERE event_ts >= current_timestamp() - INTERVAL 7 DAYS
    AND product_id IS NOT NULL
    AND event_type IN ('page_view', 'add_to_cart')
  GROUP BY product_id
),
order_signals AS (
  SELECT
    item.product_id AS product_id,
    CAST(SUM(item.quantity) AS BIGINT) AS purchases_7d
  FROM dev_main.ecom_dlt.fact_orders
  LATERAL VIEW EXPLODE(items) t AS item
  WHERE order_ts >= current_timestamp() - INTERVAL 7 DAYS
    AND item.product_id IS NOT NULL
  GROUP BY item.product_id
)
SELECT
  COALESCE(e.product_id, o.product_id) AS product_id,
  COALESCE(o.purchases_7d, 0)          AS purchases_7d,
  COALESCE(e.add_to_cart_7d, 0)        AS add_to_cart_7d,
  COALESCE(e.views_7d, 0)              AS views_7d
FROM event_signals e
FULL OUTER JOIN order_signals o ON e.product_id = o.product_id
WHERE COALESCE(e.product_id, o.product_id) IS NOT NULL
```

---

## 3 · Where ranking applies

`ProductCatalog.semantic_search` walks three tiers; ranking applies to
the two that produce a semantic score:

| Tier | Retrieval | Re-ranked? |
|---|---|---|
| 1. Vector Search | `bricksshop_vs.products_vs_index` | **Yes** |
| 2. FAISS | Volume-resident `.idx` | **Yes** |
| 3. SQL `ILIKE` | `dim_products_scd2` last-resort | **No** (no semantic axis) |

To give the ranker room to reorder, retrieval is **oversampled** — we
pull `limit * RERANK_OVERSAMPLE` (default 4×) candidates and let the
ranker pick the top `limit` after re-scoring. With `limit=5` that's a
20-row candidate pool, plenty for the ranker without hammering the
warehouse on the FAISS materialisation join.

---

## 4 · Configuration

```bash
# Weights — defaults sum to 1.0; tune per business priorities.
SEMANTIC_WEIGHT=0.60
PRICE_WEIGHT=0.15
POPULARITY_WEIGHT=0.25

# Aggregation source + window.
EVENTS_TABLE=dev_main.ecom_dlt.events
ORDERS_TABLE=dev_main.ecom_dlt.fact_orders
POPULARITY_WINDOW_DAYS=7

# Background refresh cadence (seconds). 15 min is a good baseline for
# a low-traffic demo workspace; bump down to 60–120s if popularity
# changes faster than that matters to UX.
POPULARITY_REFRESH_INTERVAL_S=900

# Required for the popularity refresh SQL (also required by the rest
# of the chat stack).
SQL_WAREHOUSE_ID=29b258f884d1ac2c
```

---

## 5 · Worked example: before vs. after

Five candidates returned by FAISS for a query like *"sturdy gaming
desk"*. All are semantically relevant; they vary in price and recent
popularity:

| pid | name              | sem  | price | popularity |
|-----|-------------------|------|-------|-----------:|
| p1  | Premium widget    | 0.92 |  $800 |       0.05 |
| p2  | Mid widget        | 0.88 |  $200 |       0.00 |
| p3  | Budget widget     | 0.81 |   $50 |       0.40 |
| p4  | Niche widget      | 0.86 |  $350 |       0.00 |
| p5  | Popular workhorse | 0.79 |  $150 |       0.95 |

### Before — pure semantic (today's behaviour without the ranker)

```
1. p1  Premium widget       sem=0.92  $800
2. p2  Mid widget           sem=0.88  $200
3. p4  Niche widget         sem=0.86  $350
4. p3  Budget widget        sem=0.81   $50
5. p5  Popular workhorse    sem=0.79  $150
```

### After — hybrid (`w_s=0.60, w_p=0.15, w_pop=0.25`)

```
1. p5  Popular workhorse    final=0.841   sem=0.79 price=0.87 pop=0.95
2. p3  Budget widget        final=0.736   sem=0.81 price=1.00 pop=0.40
3. p2  Mid widget           final=0.648   sem=0.88 price=0.80 pop=0.00
4. p4  Niche widget         final=0.606   sem=0.86 price=0.60 pop=0.00
5. p1  Premium widget       final=0.565   sem=0.92 price=0.00 pop=0.05
```

(Generated by the in-process smoke test on the live ranker — see
`web/backend/ranking.py`.)

### Impact

* The semantically-strongest candidate (`p1 Premium widget`, sem 0.92)
  drops from rank 1 to rank 5. Its price tanks the price component
  (0.00) and nobody is buying it (0.05) — its slight semantic edge
  doesn't cover the gap.
* The most popular candidate (`p5 Popular workhorse`) rises from rank 5
  to rank 1 despite the *lowest* semantic score. It's mid-priced and
  proven; the hybrid score reflects what shoppers actually pick.
* `p3 Budget widget` rises from rank 4 to rank 2: cheapest in the pool
  (price=1.00) and warm in popularity (0.40), so the price boost +
  popularity boost together overcome `p1`/`p2`'s semantic edge.

This is the headline behavioural change: the ranker replaces *"closest
text match"* with *"closest text match the shopper is most likely to
actually want"*.

---

## 6 · Operational notes

* **Cold-start neutrality.** When the popularity cache is empty
  (initial refresh hasn't run, or the warehouse is unreachable), every
  `popularity_score` is `0.0`. The hybrid score reduces to
  `w_s * semantic + w_p * price`, which is still a strict improvement
  over pure semantic — price is non-trivial information.
* **Disable per-process.** Set `POPULARITY_WEIGHT=0` and
  `PRICE_WEIGHT=0` to fall back to pure semantic without removing the
  ranker; useful for A/B comparisons.
* **Transparency.** Each `Product` carries `score` (semantic),
  `price_score`, `popularity_score`, and `final_score` after ranking.
  These flow through `Product.to_dict()` and can be surfaced on a
  debug endpoint without changing the public chat schema.
* **Observability.** `semantic_search` logs the top result's full
  score breakdown at `INFO`, e.g.:
  ```
  semantic[FAISS]: 5/20 ranked for 'sturdy gaming desk' — top final=0.841
    (sem=0.790 price=0.870 pop=0.950) pid=p5
  ```
  Grepping these lines is enough to spot a misweighted ranker in
  production.
* **Refresh cost.** One aggregate query against `events` and
  `fact_orders` per refresh interval. With the default 15-min cadence
  and the 7-day window, that's 96 lightweight aggregations per day —
  comfortably within Free Edition warehouse budgets.
