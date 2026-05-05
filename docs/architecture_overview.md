# Architecture overview

A single-page map of the BricksShop platform. Two orthogonal concerns:

* **Lakehouse pipeline** — Bronze → Silver → Gold over Delta Lake.
  Owned by the data engineering side of the project.
* **Chat stack** — multi-agent chat with tiered semantic search and a
  hybrid ranker over the same Delta tables. Owned by the AI engineering
  side.

They share storage (Unity Catalog Volumes + Delta) but never block each
other: the chat stack reads from `dim_products_scd2` and `events`; it
never writes to them. Add-to-cart events from chat re-enter the
pipeline through the same landing zone the storefront uses.

For the engineering reasoning behind individual choices, follow the
links — this page intentionally stays at the system level.

---

## 1 · The whole picture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              PRODUCERS                                  │
│                                                                         │
│   ┌────────────────────┐                ┌────────────────────────┐      │
│   │ Storefront (web)   │                │ Simulator notebook     │      │
│   │ FastAPI + JS       │                │ src/simulator/api.py   │      │
│   │ /events/batch      │                │ generate_events(...)   │      │
│   │ /chat   /chat/...  │                │ deterministic, seeded  │      │
│   └─────────┬──────────┘                └────────────┬───────────┘      │
│             │ NDJSON, partitioned by dt              │                  │
└─────────────┼─────────────────────────────────────────┼─────────────────┘
              │                                         │
              ▼                                         ▼
   ┌──────────────────────────────────────────────────────────┐
   │  /Volumes/<catalog>/ecom_bronze/landing/events/          │
   │   one immutable NDJSON file per batch                    │
   └────────────────────────────┬─────────────────────────────┘
                                │  Auto Loader (cloudFiles, schemaEvolutionMode=rescue)
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │  BRONZE   events_raw · users_raw · products_raw          │
   │           append-only · partitioned by _ingest_date      │
   └────────────────────────────┬─────────────────────────────┘
                                │  PySpark / DLT — MERGE, Window, SCD2
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │  SILVER   events · dim_users_scd2 · dim_products_scd2    │
   │           fact_orders                                    │
   └────────────────────────────┬─────────────────────────────┘
                                │  groupBy / agg / explode · OPTIMIZE ZORDER
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │  GOLD     fact_daily_sales · fact_funnel                 │
   │           fact_abandoned_carts · dim_user_360            │
   └──────────────────────────────────────────────────────────┘

                          ┌──────────────────────┐
                          │   CHAT STACK         │   reads dim_products_scd2,
                          │   (FastAPI process)  │   events, fact_orders
                          │                      │
                          │   ┌──────────────┐   │
                          │   │ Router LLM   │   │   Databricks Model Serving
                          │   └──────┬───────┘   │
                          │          ▼           │
                          │   ┌──────────────┐   │
                          │   │ Search:      │   │   Tier 1: Vector Search
                          │   │   VS → FAISS │   │   Tier 2: FAISS in Volume   ◄── current
                          │   │     → SQL    │   │   Tier 3: SQL ILIKE
                          │   └──────┬───────┘   │
                          │          ▼           │
                          │   ┌──────────────┐   │   final = 0.60·sem
                          │   │ Hybrid       │   │         + 0.15·price
                          │   │  ranker      │   │         + 0.25·popularity
                          │   └──────┬───────┘   │
                          │          ▼           │
                          │   ┌──────────────┐   │
                          │   │ Composer LLM │   │
                          │   └──────────────┘   │
                          └──────────────────────┘
                                        ▲
                                        └── chat add-to-cart re-enters
                                            the landing zone via /chat/add-to-cart
```

---

## 2 · Producers and ingestion

Two independent producers write the same NDJSON shape into the same
landing zone, distinguished only by `properties.source ∈ {"web",
"simulator"}`.

* **Storefront.** FastAPI + vanilla JS at [`web/`](../web/README.md).
  Browser tracker batches events and POSTs to `/events/batch`.
  Add-to-cart events from the chat widget go through `/chat/add-to-cart`
  and emit the same `add_to_cart` event the click path emits.
* **Simulator.** A deterministic FSM (`src/simulator/api.py`) that
  produces the same event schema. Used for backfills, integration
  tests, and the `mode=simulator` / `mode=full` orchestrator paths.

Auto Loader streams every NDJSON file into the Bronze tables.

---

## 3 · Pipeline (Bronze → Silver → Gold)

| Layer | Contract | Idempotency |
|---|---|---|
| **Bronze** | Source-faithful, append-only, no business rules. Pinned schemas; unknown fields rescued. | Auto Loader checkpoint |
| **Silver** | Cleansed, deduped, sessionised, SCD2 dimensions, fact rollups. | `MERGE` on `event_id`, hash-based SCD2 boundaries |
| **Gold** | Denormalised marts optimised for query speed. | Full `overwrite` |

Two interchangeable execution paths over the same logic:

* **Workflow `medallion`** — notebook DAG (`notebooks/00 … 99`),
  imports from `src/`. Drives the daily batch run.
* **DLT `bricksshop-dlt`** — declarative `@dlt.table` views in
  `pipelines/dlt/`. Self-contained (no `src/` import), runs on
  serverless, target schema `${var.dlt_schema}` so it never contends
  on the Workflow path's tables.

Full layer docs:
[`bronze_layer.md`](./bronze_layer.md),
[`silver_layer.md`](./silver_layer.md),
[`gold_layer.md`](./gold_layer.md),
[`dlt_pipeline.md`](./dlt_pipeline.md).

---

## 4 · Chat stack

A FastAPI process that hosts the existing event endpoints **plus**:

* `POST /chat` — multi-agent reply.
* `POST /chat/add-to-cart` — emits a real `add_to_cart` Delta event.

Inside the process:

1. **Router LLM** (Databricks Model Serving) classifies the user's
   intent and extracts a clean search query + filters.
2. **`ProductCatalog.semantic_search`** runs the three-tier search
   chain. See [`search.md`](./search.md).
3. **`HybridRanker`** re-scores the retrieved candidates using
   semantic + price + popularity signals. See [`ranking.md`](./ranking.md).
4. **Composer LLM** turns the ranked products into a natural-language
   reply.

The chat stack is read-only against the catalog (`dim_products_scd2`,
`events`, `fact_orders`); the only writes it produces flow back through
the storefront's `/events/batch` path, so the lakehouse pipeline picks
them up like any other event source.

---

## 5 · Search hierarchy — and why FAISS

```
Tier 1   Vector Search   (intended production path)
Tier 2   FAISS           (currently serves traffic — Volume + hot reload)
Tier 3   SQL ILIKE       (last-resort literal match on dim_products_scd2)
```

**The platform constraint.** Free Edition workspaces don't expose
Vector Search endpoints. We kept the Tier-1 integration in code
because that's where this stack lands once an endpoint is available
— but we built FAISS as a **production-ready fallback**, not a
prototype:

* a sentence-transformer (`all-MiniLM-L6-v2`, dim 384) runs inside
  the FastAPI process,
* the FAISS `IndexFlatIP` lives on a Unity Catalog Volume
  (`/Volumes/dev_main/ecom_artifacts/faiss/`),
* a pointer file is rewritten last by every rebuild, so a partial
  rebuild never poisons the latest pointer,
* the backend polls the pointer and **hot-reloads** the index without
  a restart,
* the embeddings Delta table carries `embedding_model_name` +
  `embedding_version` per row for full auditability.

When Vector Search is provisioned, set `VS_ENDPOINT` / `VS_INDEX_NAME`
and FAISS becomes a true fallback — no code change. Full deep dive in
[`search.md`](./search.md).

---

## 6 · Hybrid ranking

```
final_score = 0.60 · semantic_similarity     ← from VS / FAISS, clipped to [0,1]
            + 0.15 · price_score             ← per-query inverse min-max
            + 0.25 · popularity_score        ← log-norm of 7-day events + orders
```

* **Popularity** is computed from `fact_orders` and `events` over a
  7-day rolling window (configurable). Refreshed in a daemon thread
  every `POPULARITY_REFRESH_INTERVAL_S` (default 900s) — chat replies
  never block on this SQL.
* **Price** is candidate-relative: cheapest in the pool → 1.0,
  priciest → 0.0. This keeps the price signal carrying full weight
  whether the user is buying $20 toasters or $5,000 sofas.
* **Weights are env-tunable.** Set `POPULARITY_WEIGHT=0` and
  `PRICE_WEIGHT=0` to fall back to pure semantic without removing the
  ranker — useful for A/B comparisons.

Full deep dive: [`ranking.md`](./ranking.md).

---

## 7 · Orchestration

Single bundle job (`ecom_orchestrator`) parameterised by `mode`:

| `mode`      | Simulator | DLT pipeline | Use |
|---|---|---|---|
| `prod` (default) | skipped | runs | production |
| `simulator`      | runs    | skipped | backfill |
| `full`           | runs    | runs after | end-to-end demo |

The simulator notebook is **always-run** but reads `mode` and exits
as a no-op when `mode=prod` — this avoids a Databricks Jobs
limitation where `outcome:` qualifiers are silently ignored under
`run_if: ALL_DONE`. See [`runbook.md`](./runbook.md) §1 for the
canonical execution path.

Search-stack jobs:

* `bricksshop_faiss_rebuild` (daily 03:30 UTC, paused) — rebuilds the
  embeddings Delta table + FAISS index + pointer.

---

## 8 · How the system scales

The architecture is designed to **substitute components, not rewrite
them**. The table below maps every constraint we hit on Free Edition
to its production replacement, plus the boundary that already exists
in the codebase to make the swap trivial.

| Component | Current (Free Edition) | Future (production) | What changes in code |
|---|---|---|---|
| **Tier 1 retrieval** | FAISS in-process serves; Vector Search (Tier 1) integration is dormant | Vector Search managed endpoint (Tier 1 lights up) | Set `VS_ENDPOINT` + `VS_INDEX_NAME`. `ProductCatalog` already prefers Tier 1 — FAISS becomes a true fallback. |
| **Tier 2 retrieval** | FAISS index loaded inside the FastAPI process | FAISS retired *or* kept as warm fallback for VS outages | Optional. Both paths share `(product_id, score)` shape — chat code is unchanged. |
| **Popularity signal** | In-memory polled cache, refreshed by a daemon thread (one SQL query / 15 min) | Shared Delta table (`product_popularity`) refreshed by a scheduled job; backend reads the table | Replace `PopularitySignals.refresh()` with a Delta read; ranker is unchanged. |
| **API process** | Single `uvicorn` instance | Horizontally scalable service behind a load balancer | Each replica polls the FAISS pointer + popularity Delta independently. No sticky sessions needed — chat is stateless except for `ConversationStore`, which moves to Redis. |
| **Producer** | Web app + simulator notebook | Web app + Kafka / Kinesis / Event Hubs | Auto Loader points at the streaming source instead of the Volume. Bronze/Silver/Gold unchanged. |
| **Compute** | Shared cluster + serverless DLT | Per-layer job clusters · Photon · spot · serverless DLT | `databricks.yml` cluster config; pipeline code unchanged. |
| **Schema governance** | `_rescued_data` rescue column | Confluent / Apicurio Schema Registry on the producer | Bronze schema unchanged; Auto Loader contract is the same. |

### Why the substitution is cheap

* **Three-tier search is interface-driven.** `ProductCatalog` calls
  Tier 1, Tier 2, Tier 3 through identical method shapes. Adding a
  Vector Search endpoint flips a tier from "code path waiting for
  config" to "live tier" without touching the catalog.
* **Hybrid ranking sits behind a single class.** `HybridRanker.rank()`
  takes a `Sequence[Product]` and returns the same shape — its source
  of popularity numbers (cache vs. Delta) is hidden behind one
  interface (`PopularitySignals.popularity_score(pid)`).
* **The FAISS rebuild is already a Databricks Job.** Promoting it
  from "manual + scheduled" to "triggered by upstream DLT update"
  is one `databricks.yml` change, not a rewrite.

No architectural rewrite required for any of these moves. That's the
entire point of the layering.

---

## 9 · Storage map

| Path / table | Owner | Purpose |
|---|---|---|
| `/Volumes/<cat>/ecom_bronze/landing/events/dt=…/` | producers | NDJSON landing zone |
| `<cat>.ecom_bronze.events_raw` | Bronze | append-only event capture |
| `<cat>.ecom_silver.events` | Silver | deduped, sessionised events |
| `<cat>.ecom_silver.dim_products_scd2` | Silver | product master, SCD2 |
| `<cat>.ecom_silver.fact_orders` | Silver | per-order rollup with line items |
| `<cat>.ecom_gold.fact_daily_sales` | Gold | daily GMV/units/AOV |
| `<cat>.ecom_gold.fact_funnel` | Gold | view → cart → purchase rates |
| `<cat>.ecom_dlt.product_embeddings` | search build | per-product embedding + audit cols |
| `/Volumes/<cat>/ecom_artifacts/faiss/faiss_index_v{ts}.idx` | search build | versioned FAISS files |
| `/Volumes/<cat>/ecom_artifacts/faiss/faiss_index_latest.txt` | search build | hot-reload pointer |

---

## 10 · Where to read next

| You want to | Read |
|---|---|
| Understand Bronze ingestion in depth | [`bronze_layer.md`](./bronze_layer.md) |
| Understand Silver dedup / SCD2 | [`silver_layer.md`](./silver_layer.md) |
| Understand Gold marts | [`gold_layer.md`](./gold_layer.md) |
| Run / debug the pipeline | [`runbook.md`](./runbook.md) |
| Understand the data quality gate | [`data_quality.md`](./data_quality.md) |
| Pin schemas / SCD2 rules | [`data_model.md`](./data_model.md) |
| Run the search stack end-to-end | [`search.md`](./search.md) |
| Tune the ranker | [`ranking.md`](./ranking.md) |
| Set up CI/CD | [`ci_cd.md`](./ci_cd.md) |
| See the full design rationale (data side) | [`architecture.md`](./architecture.md) |
