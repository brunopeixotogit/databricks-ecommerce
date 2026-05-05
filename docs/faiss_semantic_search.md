# FAISS semantic search

This document covers the **FAISS fallback tier** of the BricksShop chat
search stack. It is the second of three tiers `ProductCatalog.semantic_search`
walks before degrading to a SQL `ILIKE`:

```
1. Databricks Vector Search           (tier 1 — production)
2. FAISS index in a Volume            (tier 2 — this doc)
3. SQL ILIKE on dim_products_scd2     (tier 3 — last resort)
```

The FAISS tier is self-contained: a sentence-transformer model runs
inside the FastAPI process, a FAISS `IndexFlatIP` lives on a Databricks
Volume, and a tiny pointer file lets the backend hot-reload without a
restart.

---

## 1 · Architecture

```
            ┌──────────────────────────── BUILD JOB ────────────────────────────┐
            │                                                                   │
  Delta     │   dim_products_scd2 (current/active)                              │
  source    │            │                                                      │
            │            ▼                                                      │
            │   Embedder(sentence-transformers/all-MiniLM-L6-v2,                │
            │            L2-normalised, dim=384)                                │
            │            │                                                      │
            │            ├──▶ JSONL → COPY INTO product_embeddings              │
            │            │     (product_id, doc_text, embedding,                │
            │            │      embedding_model_name, embedding_version,       │
            │            │      embed_ts)                                       │
            │            │                                                      │
            │            └──▶ faiss.IndexFlatIP                                 │
            │                       │                                           │
            │                       ▼                                           │
            │     /Volumes/.../faiss/faiss_index_v{ts}.idx     (versioned)      │
            │                       │                                           │
            │                       ▼   (only after .idx upload succeeds)       │
            │     /Volumes/.../faiss/faiss_index_latest.txt    (pointer JSON)   │
            │                                                                   │
            └───────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼  poll every FAISS_POLL_INTERVAL_S
            ┌───────────────────────── BACKEND ─────────────────────────────────┐
            │                                                                   │
            │   FaissProductIndex                                               │
            │     ├─ reads pointer, checks version + model + dim                │
            │     ├─ downloads .idx, faiss.read_index, atomic swap              │
            │     ├─ joins row→product_id via embeddings table                  │
            │     └─ search(query_text) → list[FaissHit]                        │
            │              │                                                    │
            │              ▼                                                    │
            │   ProductCatalog.semantic_search                                  │
            │     VS → FAISS → SQL fallback chain                               │
            │              │                                                    │
            │              ▼                                                    │
            │   /chat endpoint (FastAPI) → frontend chat UI                     │
            │                                                                   │
            └───────────────────────────────────────────────────────────────────┘
```

Key invariants:

* **Pointer is written last.** A partial run never poisons
  `faiss_index_latest.txt` — readers either see the old version or the
  new one, never a pointer to a half-uploaded `.idx`.
* **Versioned filenames.** `faiss_index_v{ts}.idx` accumulates; the
  `--keep-versions N` flag prunes older copies. Rolling back is just
  rewriting the pointer to a previous filename.
* **Model + dim guardrail.** The pointer carries
  `embedding_model_name` and `embedding_dim`; the loader refuses to
  serve an index that disagrees with the runtime `Embedder`. This
  prevents the "we changed the model but forgot to rebuild" foot-gun.
* **Hot reload.** A daemon thread polls the pointer; when `version`
  changes it loads the new index and atomically swaps it in. In-flight
  queries hold a local reference to the previous `_LoadedIndex`, so
  reload never blocks readers.

---

## 2 · Modules

| Path | Responsibility |
|------|---------------|
| `web/backend/embeddings.py`     | Single source of truth for model + dim. Lazy, thread-safe `Embedder`. |
| `web/backend/faiss_index.py`    | Loader, hot-reload poller, `search()`. |
| `web/backend/products.py`       | `ProductCatalog.semantic_search` — VS → FAISS → SQL chain + filter join. |
| `web/backend/app.py`            | Lifespan wires `FaissProductIndex` into the catalog. |
| `pipelines/faiss/setup_volume.py` | Creates the schema + Volume (one-shot). |
| `pipelines/faiss/build.py`      | End-to-end rebuild — embeddings table + `.idx` + pointer. |
| `notebooks/faiss_rebuild.py`    | Notebook entry point used by the bundle job. |
| `databricks.yml`                | `bricksshop_faiss_rebuild` job (scheduled). |

---

## 3 · Embeddings Delta table

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
    embedding_version     STRING                  -- str(int(time.time())) — same as filename ts
    embed_ts              TIMESTAMP
```

`embedding_model_name` and `embedding_version` are written **on every
row** so the contract is auditable from SQL alone:

```sql
SELECT embedding_model_name, embedding_version, COUNT(*)
FROM dev_main.ecom_dlt.product_embeddings
GROUP BY 1, 2;
```

---

## 4 · FAISS pointer file

`/Volumes/dev_main/ecom_artifacts/faiss/faiss_index_latest.txt` is a
JSON document the build job rewrites at the end of every successful
run:

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

The backend treats `version` as the cache key. When it changes, the
loader downloads the new `.idx`, validates model + dim, joins
`product_id`s in build order, and atomically swaps the in-memory index.

---

## 5 · Running the rebuild

### 5.1 — One-shot: provision the Volume

Run once (or any time you need to reset the artifact store):

```bash
export DATABRICKS_HOST=https://...cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
export SQL_WAREHOUSE_ID=29b258f884d1ac2c

python -m pipelines.faiss.setup_volume
```

Creates `dev_main.ecom_artifacts.faiss` (schema + volume).

### 5.2 — Local CLI rebuild

```bash
pip install -r web/backend/requirements.txt    # faiss-cpu + sentence-transformers
python -m pipelines.faiss.build --progress
```

The first run downloads the `all-MiniLM-L6-v2` weights (~80 MB) into
the HF cache.

### 5.3 — Databricks notebook rebuild

Open `notebooks/faiss_rebuild.py` and **Run all**. The notebook
`%pip install`s the deps and calls `pipelines.faiss.build.build()`.

### 5.4 — Databricks Job (scheduled)

The bundle declares a job — deploy once, then unpause:

```bash
databricks bundle deploy --target dev
databricks bundle run    --target dev bricksshop_faiss_rebuild
```

The job is currently `pause_status: PAUSED` in `databricks.yml`. Flip
it to `UNPAUSED` to enable the daily 03:30 UTC schedule. Definition:

```yaml
bricksshop_faiss_rebuild:
  name: bricksshop-faiss-rebuild
  max_concurrent_runs: 1
  tasks:
    - task_key: rebuild_index
      notebook_task:
        notebook_path: notebooks/faiss_rebuild.py
  schedule:
    quartz_cron_expression: "0 30 3 * * ?"
    timezone_id: UTC
    pause_status: PAUSED
```

---

## 6 · Backend behaviour

The FastAPI lifespan (`web/backend/app.py`) constructs
`FaissProductIndex()` and calls `start()`. `start()` is best-effort:

* Pointer found and matches model/dim → index loaded, tier active.
* Pointer missing / mismatched → index stays unloaded, the poller
  keeps retrying every `FAISS_POLL_INTERVAL_S` seconds. The next
  rebuild's pointer will be picked up automatically.

`ProductCatalog.semantic_search` walks the tiers and falls through on
either an `Unavailable` exception OR an empty hit list:

```
Vector Search → (unavailable | 0 hits) → FAISS → (unavailable | 0 hits) → SQL ILIKE
```

Filters (`category`, `max_price`) are applied **after** the FAISS hit
list via a SQL join on `dim_products_scd2`. We oversample the FAISS
search by 5× so the post-filter still has enough candidates to satisfy
the requested limit.

---

## 7 · Verifying end-to-end

```bash
# 1. Provision (once)
python -m pipelines.faiss.setup_volume

# 2. Build
python -m pipelines.faiss.build

# Expected log tail:
#   Refreshed dev_main.ecom_dlt.product_embeddings (N rows, model=..., version=...)
#   Built IndexFlatIP (dim=384, n=N)
#   Uploaded /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_v....idx
#   Pointer updated: ...
#   BUILD OK: {...}

# 3. Inspect the Delta table
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "'$SQL_WAREHOUSE_ID'",
  "statement": "SELECT embedding_model_name, embedding_version, COUNT(*) FROM dev_main.ecom_dlt.product_embeddings GROUP BY 1,2"
}'

# 4. Inspect the pointer
databricks fs cat /Volumes/dev_main/ecom_artifacts/faiss/faiss_index_latest.txt

# 5. Start the backend and chat
uvicorn web.backend.app:app --env-file web/config/.env --reload

# Expected log line:
#   FAISS tier active: {'ready': True, 'version': '...', 'embedding_model_name': '...', ...}

# 6. Hit /chat from the UI (http://localhost:8000) — when Vector Search
#    is unavailable the chat reply is served from the FAISS tier.
```

To confirm hot reload, run `python -m pipelines.faiss.build` again
without restarting uvicorn. Within `FAISS_POLL_INTERVAL_S` the backend
logs:

```
FAISS index loaded (poll): version=<new>  n=<N>  [replaced v<old>]
```

---

## 8 · Configuration reference

All knobs come from environment variables — see
`web/config/.env.example`.

| Variable | Default | Used by |
|---|---|---|
| `DATABRICKS_HOST`         | —                                         | both |
| `DATABRICKS_TOKEN`        | —                                         | both |
| `SQL_WAREHOUSE_ID`        | —                                         | both (catalog + row→pid join) |
| `FAISS_VOLUME_PATH`       | `/Volumes/dev_main/ecom_artifacts/faiss`  | both |
| `FAISS_POLL_INTERVAL_S`   | `60`                                      | backend |
| `EMBEDDINGS_TABLE`        | `dev_main.ecom_dlt.product_embeddings`    | both |
| `EMBED_MODEL_NAME`        | `sentence-transformers/all-MiniLM-L6-v2`  | both |
| `EMBED_DIM`               | `384`                                     | both |
| `FAISS_KEEP_VERSIONS`     | `5`                                       | build only |

---

## 9 · Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Backend log: `FAISS tier idle (pointer not ready)` | No build has run yet, or the Volume path is wrong | Run §5.2 / §5.3, or set `FAISS_VOLUME_PATH` |
| Backend log: `model mismatch: index=… runtime=…` | The build used a different model than the backend has loaded | Align `EMBED_MODEL_NAME` on both sides, or rebuild |
| Backend log: `dim mismatch: index=… runtime=…` | Same as above (dim derived from model) | Same |
| Build log: `No products to embed — refusing to publish an empty index.` | DLT has not produced `dim_products_scd2`, or all rows are inactive | Run the medallion / DLT pipeline first |
| Build log: `row count mismatch: pids=N faiss=M` | The embeddings table changed concurrently with the index build | Re-run; rebuilds are idempotent |
| Search returns 0 hits with high-quality query | Filters dropped them all | Increase `oversample` or relax `category`/`max_price` |
