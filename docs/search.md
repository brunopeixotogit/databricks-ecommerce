# Semantic search

How the BricksShop chat layer turns a free-text query into a list of
products.

## Tiered retrieval — TL;DR

The chat layer never single-points-of-failure on a single retrieval
backend. `ProductCatalog.semantic_search` walks three tiers and stops
at the first one that returns hits:

| Tier | Backend | Status | Where it lives |
|---|---|---|---|
| **Tier 1** | **Vector Search (intended)** | Wired in code; activates when an endpoint is provisioned | `web/backend/vector_search.py`, `pipelines/vector_search/setup.py` |
| **Tier 2** | **FAISS (current)** | **Currently serves traffic** — hot-reloadable index in a Databricks Volume | `web/backend/faiss_index.py`, `pipelines/faiss/build.py` |
| **Tier 3** | **SQL ILIKE (fallback)** | Last-resort literal match on `dim_products_scd2` | `web/backend/products.py` (`ProductCatalog.search`) |

```
user query
    │
    ▼
┌────────────────────────────────────────────────────────────────────┐
│  ProductCatalog.semantic_search                                    │
│                                                                    │
│   ┌────────────┐   miss / unavailable   ┌────────────┐   miss /    │
│   │ Tier 1     │ ──────────────────────▶│ Tier 2     │  unavailable│
│   │ Vector     │                        │ FAISS      │ ─────────┐  │
│   │ Search     │                        │ (Volume)   │          │  │
│   └─────┬──────┘                        └─────┬──────┘          │  │
│         │ hits                                │ hits            │  │
│         ▼                                     ▼                 │  │
│      Hybrid ranker                         Hybrid ranker        │  │
│         │                                     │                 │  │
│         └──────────────┬──────────────────────┘                 │  │
│                        │                                        │  │
│                        ▼                              ┌─────────▼──┐│
│                   top-N products                      │ Tier 3     ││
│                        │                              │ SQL ILIKE  ││
│                        ▼                              │ on dim_*   ││
│                  /chat reply                          └────────────┘│
└────────────────────────────────────────────────────────────────────┘
```

Retrieval is unchanged across the tiers — each one returns scored
candidates ranked by cosine similarity (Tier 1, Tier 2) or a `LIKE`
match (Tier 3, no score). The hybrid ranker (see
[`ranking.md`](./ranking.md)) re-orders the semantic tiers' output by
`0.60·semantic + 0.15·price + 0.25·popularity`. **Tier 3 is unranked**
— there's no semantic axis to combine with.

The chain degrades transparently — the catalog logs which tier served
each query, and the chat layer never sees an error from the search
stack unless all three tiers fail.

---

## 1 · Why three tiers

The retrieval stack reflects a real engineering constraint, not theory.

**Tier 1 — Databricks Vector Search.** The intended production path:
managed index, sync-from-Delta, server-side filters, no model loaded
in the FastAPI process. We keep the integration in
[`web/backend/vector_search.py`](../web/backend/vector_search.py) and
[`pipelines/vector_search/setup.py`](../pipelines/vector_search/setup.py)
because that's where this stack will land once Vector Search endpoints
are available in the workspace.

**Tier 2 — FAISS in a Volume.** The tier that actually serves traffic
today. **Free Edition workspaces don't expose Vector Search endpoints**,
so we built a self-contained alternative: a sentence-transformer running
inside the FastAPI process, encoding queries against a pre-built FAISS
`IndexFlatIP` file checked out of a Unity Catalog Volume. It produces
the same `(product_id, score)` shape as Vector Search, so the catalog
treats them as interchangeable.

**Tier 3 — SQL `ILIKE`.** A last-resort path that works even when
nothing semantic is loaded. Useful during cold start, when both
Vector Search and FAISS are unavailable, or for queries we know are
trivially literal (an exact product code).

The chain degrades transparently — the catalog logs which tier served
each query, and the chat layer never sees an error from the search
stack unless all three tiers fail.

---

## 2 · Why FAISS is the right fallback

Picking FAISS as the fallback was a deliberate engineering decision,
not a placeholder. The alternatives we rejected:

| Option | Why we didn't pick it |
|---|---|
| Hosted vector DB (Pinecone, Weaviate, …) | Requires an external account + secret + network egress. Adds a third trust boundary for a fallback we expect to use only when Vector Search is unavailable. |
| Vector extension on the warehouse | Free Edition warehouses don't ship `pgvector`-equivalent extensions; emulating cosine similarity with array math is too slow on a 1k+ catalog. |
| Brute-force cosine in Python | Same model load + encode cost as FAISS, but linear scan instead of `IndexFlatIP`. FAISS wins by ~10× on small catalogs and grows further from there. |

FAISS gives us:

* **Zero external dependencies.** The index is a single file on a
  Unity Catalog Volume the workspace already owns.
* **Hot reload without a restart.** A pointer file
  (`faiss_index_latest.txt`) is rewritten last by every rebuild; the
  backend polls it every `FAISS_POLL_INTERVAL_S` (default 60s) and
  swaps the in-memory index atomically.
* **Audit trail.** The embeddings Delta table carries
  `embedding_model_name` + `embedding_version` per row, so any
  `SELECT` against it answers *"which model produced these vectors,
  and when"*.
* **Model + dim guardrail.** The pointer also carries the model name
  and dimension; the loader refuses to serve an index whose
  embeddings disagree with the model the backend has loaded.

When Vector Search becomes available, we promote it to Tier 1 by
setting `VS_ENDPOINT` / `VS_INDEX_NAME` and FAISS automatically
becomes a true fallback — no code change.

---

## 3 · Build pipeline

End-to-end flow of the FAISS rebuild — idempotent, safe to re-run:

```
Delta dim_products_scd2 (current/active rows)
    │  Statement Execution API
    ▼
Embedder (sentence-transformers/all-MiniLM-L6-v2, L2-normalised, dim=384)
    │
    ├──▶ JSONL on Volume → COPY INTO dev_main.ecom_dlt.product_embeddings
    │     (product_id, doc_text, embedding,
    │      embedding_model_name, embedding_version, embed_ts)
    │
    └──▶ faiss.IndexFlatIP
              │
              ▼
    /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_v{ts}.idx   (versioned)
              │
              ▼   (only after .idx upload succeeds)
    /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_latest.txt  (JSON pointer)
```

Two invariants the pointer enforces:

* **Pointer is written last.** Readers either see the old version or
  the new one — never a pointer to a half-uploaded `.idx`.
* **Versioned filenames.** `faiss_index_v{ts}.idx` accumulates;
  `--keep-versions N` (or `FAISS_KEEP_VERSIONS`) prunes older copies.
  Rolling back is one pointer rewrite.

---

## 4 · Embeddings Delta table

Created/replaced by every rebuild:

```
catalog : dev_main
schema  : ecom_dlt
table   : product_embeddings
columns :
    product_id            STRING NOT NULL
    doc_text              STRING
    embedding             ARRAY<FLOAT>            -- L2-normalised, dim=384
    embedding_model_name  STRING                  -- "sentence-transformers/all-MiniLM-L6-v2"
    embedding_version     STRING                  -- str(int(time.time())) — same ts as the .idx file
    embed_ts              TIMESTAMP
```

Auditability check from SQL alone:

```sql
SELECT embedding_model_name, embedding_version, COUNT(*)
FROM dev_main.ecom_dlt.product_embeddings
GROUP BY 1, 2;
```

---

## 5 · Pointer file

`/Volumes/dev_main/ecom_artifacts/faiss/faiss_index_latest.txt`:

```json
{
  "filename":             "faiss_index_v1746389820.idx",
  "version":              "1746389820",
  "embedding_model_name": "sentence-transformers/all-MiniLM-L6-v2",
  "embedding_dim":        384,
  "n_vectors":            1247,
  "built_at":             "2026-05-04T19:37:00Z"
}
```

`version` is the cache key. When it changes, the loader downloads the
new `.idx`, validates model + dim, joins `product_id`s in build order
from `product_embeddings`, and atomically swaps the in-memory index.

---

## 6 · Modules

| Path | Responsibility |
|------|---------------|
| `web/backend/embeddings.py`     | Single source of truth for model + dim. Lazy, thread-safe `Embedder`. |
| `web/backend/faiss_index.py`    | Loader, hot-reload poller, `search()`. |
| `web/backend/vector_search.py`  | Tier-1 wrapper; degrades cleanly when the workspace doesn't have an endpoint. |
| `web/backend/products.py`       | `ProductCatalog.semantic_search` — VS → FAISS → SQL chain + filter join. |
| `web/backend/ranking.py`        | Hybrid re-ranker applied after Tier 1 / Tier 2. |
| `web/backend/app.py`            | Lifespan wires `FaissProductIndex` and `HybridRanker` into the catalog. |
| `pipelines/faiss/setup_volume.py` | Idempotent schema + Volume creation. |
| `pipelines/faiss/build.py`      | Embeddings table + `.idx` + pointer + retention. |
| `notebooks/faiss_rebuild.py`    | Notebook entry point used by the bundle job. |
| `databricks.yml` (`bricksshop_faiss_rebuild`) | Daily 03:30 UTC scheduled job (paused by default). |

---

## 7 · Running the rebuild

### 7.1 — One-shot: provision the Volume

```bash
export DATABRICKS_HOST=https://...cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
export SQL_WAREHOUSE_ID=29b258f884d1ac2c
python -m pipelines.faiss.setup_volume
```

### 7.2 — Local CLI rebuild

```bash
pip install -r web/backend/requirements.txt
python -m pipelines.faiss.build --progress
```

First run downloads the `all-MiniLM-L6-v2` weights (~80 MB) into the
HF cache.

### 7.3 — Databricks notebook rebuild

Open `notebooks/faiss_rebuild.py` and **Run all**. The notebook
`%pip install`s the deps and calls `pipelines.faiss.build.build()`.

### 7.4 — Databricks Job (scheduled)

```bash
databricks bundle deploy --target dev
databricks bundle run    --target dev bricksshop_faiss_rebuild
```

Currently `pause_status: PAUSED` in `databricks.yml` — flip to
`UNPAUSED` to enable the daily 03:30 UTC schedule.

---

## 8 · Backend behaviour

The FastAPI lifespan constructs `FaissProductIndex()` and calls
`start()`. `start()` is best-effort:

* Pointer found and matches model/dim → index loaded, tier active.
* Pointer missing / mismatched → index stays unloaded, the poller
  keeps retrying every `FAISS_POLL_INTERVAL_S` seconds.

Filters (`category`, `max_price`) are applied **after** the FAISS hit
list via a SQL join on `dim_products_scd2`. Retrieval oversamples by
`RERANK_OVERSAMPLE` (default 4×) so the post-filter and the hybrid
ranker have enough candidates to satisfy the requested limit.

---

## 9 · Configuration reference

All knobs are environment variables — see
[`web/config/.env.example`](../web/config/.env.example).

| Variable | Default | Used by |
|---|---|---|
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | — | both tiers + build |
| `SQL_WAREHOUSE_ID`        | —                                         | catalog + row→pid join + popularity refresh |
| `VS_ENDPOINT` / `VS_INDEX_NAME` | `bricksshop_vs` / `dev_main.ecom_dlt.products_vs_index` | Tier 1 |
| `FAISS_VOLUME_PATH`       | `/Volumes/dev_main/ecom_artifacts/faiss`  | Tier 2 |
| `FAISS_POLL_INTERVAL_S`   | `60`                                      | Tier 2 hot reload |
| `EMBEDDINGS_TABLE`        | `dev_main.ecom_dlt.product_embeddings`    | Tier 2 build + load |
| `EMBED_MODEL_NAME`        | `sentence-transformers/all-MiniLM-L6-v2`  | Tier 2 |
| `EMBED_DIM`               | `384`                                     | Tier 2 |
| `FAISS_KEEP_VERSIONS`     | `5`                                       | build only |

---

## 10 · End-to-end verification

```bash
# 1. Provision (once)
python -m pipelines.faiss.setup_volume

# 2. Build
python -m pipelines.faiss.build

# Expected log tail:
#   Refreshed dev_main.ecom_dlt.product_embeddings (N rows, model=..., version=...)
#   Built IndexFlatIP (dim=384, n=N)
#   Uploaded /Volumes/.../faiss_index_v....idx
#   Pointer updated: ...
#   BUILD OK: {...}

# 3. Inspect the pointer
databricks fs cat /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_latest.txt

# 4. Start the backend
uvicorn web.backend.app:app --env-file web/config/.env --reload

# Expected log lines:
#   FAISS tier active: {'ready': True, 'version': '...', ...}
#   Hybrid ranking active: weights={'semantic': 0.6, 'price': 0.15, 'popularity': 0.25} ...

# 5. Hit /chat from the UI (http://localhost:8000) — when Vector Search
#    is unavailable the chat reply is served from FAISS, hybrid-ranked.
```

To confirm hot reload, run `python -m pipelines.faiss.build` again
without restarting uvicorn. Within `FAISS_POLL_INTERVAL_S` the backend
logs:

```
FAISS index loaded (poll): version=<new>  n=<N>  [replaced v<old>]
```

---

## 11 · Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Backend log: `FAISS tier idle (pointer not ready)` | No build has run yet, or `FAISS_VOLUME_PATH` is wrong | Run §7.2 / §7.3, or fix the env var |
| Backend log: `model mismatch: index=… runtime=…` | The build used a different model than the backend has loaded | Align `EMBED_MODEL_NAME` on both sides, or rebuild |
| Backend log: `dim mismatch: index=… runtime=…` | Same as above (dim derived from model) | Same |
| Build log: `No products to embed — refusing to publish an empty index.` | DLT has not produced `dim_products_scd2`, or all rows are inactive | Run the medallion / DLT pipeline first |
| Build log: `row count mismatch: pids=N faiss=M` | The embeddings table changed concurrently with the index build | Re-run; rebuilds are idempotent |
| Search returns 0 hits with high-quality query | Filters dropped the candidate pool | Increase `RERANK_OVERSAMPLE`, or relax `category`/`max_price` |
| Vector Search hits are returned but ranked weird | `popularity_score=0` for everything (cache cold) | Wait one `POPULARITY_REFRESH_INTERVAL_S`, or check the warehouse |
