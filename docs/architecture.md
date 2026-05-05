# Architecture

End-to-end design of the e-commerce lakehouse on **Databricks + Delta Lake**, organised as a Bronze → Silver → Gold medallion. This document explains the system shape, the data flow, and — most importantly — the *reasoning* behind each structural choice.

> **Looking for the one-page system map?** See [`architecture_overview.md`](./architecture_overview.md) — it covers both this lakehouse pipeline and the chat / search / ranking stack on top of it.

> Companion docs: [`bronze_layer.md`](./bronze_layer.md), [`silver_layer.md`](./silver_layer.md), [`gold_layer.md`](./gold_layer.md), [`data_model.md`](./data_model.md), [`data_quality.md`](./data_quality.md), [`ci_cd.md`](./ci_cd.md), [`runbook.md`](./runbook.md), [`search.md`](./search.md), [`ranking.md`](./ranking.md).

---

## 1 · System overview

A synthetic e-commerce site (electronics, appliances, furniture) emits clickstream events and entity snapshots into a Unity Catalog Volume. Auto Loader streams those files into **append-only Bronze** Delta tables. Silver enforces business invariants — dedup, sessionisation, SCD2 — and Gold projects four denormalised marts that feed BI, CRM, and ML feature engineering.

```
┌──────────────────────┐
│  Python Simulator    │  src/simulator/  · deterministic, seeded
│  (FSM + faker + RNG) │
└──────────┬───────────┘
           │ NDJSON.gz, partitioned by dt=YYYY-MM-DD
           ▼
   /Volumes/main/ecom_bronze/landing/{events,users,products}/
           │
           │  Auto Loader (cloudFiles · schemaEvolutionMode=rescue · trigger=availableNow)
           ▼
┌──────────── BRONZE ────────────┐  append-only · partitioned by _ingest_date
│ events_raw  users_raw  products_raw │
└──────────┬─────────────────────┘
           │  PySpark · Delta MERGE · Window functions
           ▼
┌──────────── SILVER ────────────┐  dedup · sessionise · SCD2 · facts
│ events     dim_users_scd2      │
│ fact_orders  dim_products_scd2 │
└──────────┬─────────────────────┘
           │  groupBy / agg / explode · OPTIMIZE ZORDER
           ▼
┌──────────── GOLD ──────────────┐  BI / CRM / ML feature inputs
│ fact_daily_sales      fact_funnel    │
│ fact_abandoned_carts  dim_user_360   │
└──────────┬─────────────────────┘
           ▼
   ┌──────── 99_quality_checks ────────┐
   │ Expectation gate; fail aborts run │
   └───────────────────────────────────┘
```

---

## 2 · Layer contracts

| Layer | Responsibility | Storage shape | Idempotency mechanism |
|---|---|---|---|
| **Bronze** | Source-faithful capture, no business rules. Pinned schemas; unknown fields → `_rescued_data`. | Delta, append, `partitionBy(_ingest_date)` | Auto Loader checkpoint (`_checkpoints/<source>`) |
| **Silver** | Cleansed, deduped, conformed, sessionised, SCD2 dimensions, fact rollups. | Delta — `MERGE` for events, `overwrite` for SCD2 + facts | Delta `MERGE` on `event_id`; SCD2 hash boundaries |
| **Gold**   | Denormalised marts for query speed; BI / CRM / ML feature inputs. | Delta, `overwrite` with `overwriteSchema=true` | Full rebuild from Silver |

The contract between layers is the **Delta table schema** — a stable target both producers (the layer below) and consumers (the layer above) can rely on. Silver does not read from the Volume; Gold does not read from Bronze; the layering is enforced by code, not by convention.

---

## 3 · Design principles

These principles govern every concrete decision in the codebase.

### 3.1 Decoupled producer
The simulator writes plain JSON files into a Volume. The ingestion layer doesn't know whether the data is synthetic. Replacing it with Kafka, Kinesis, or Event Hubs changes ~50 lines in `src/bronze/ingest_events.py`; everything else stays identical.

**Why:** in production, the producer is owned by a different team or third party. Pretending otherwise during development bakes in coupling that's painful to remove later.

### 3.2 Append-only Bronze
Bronze is the **replay log**. Nothing modifies or deletes Bronze rows — the Delta table property `delta.appendOnly=true` enforces this.

**Why:** Silver and Gold must always be rebuildable. If Bronze gets mutated, debugging "why did yesterday's GMV change today" becomes impossible. The replay-log contract turns the lakehouse into a fully reproducible system.

### 3.3 Pinned schemas, no `inferSchema`
Every source has a `StructType` in `src/common/schemas.py`, plus a `SCHEMA_VERSION` constant. Auto Loader is configured with `cloudFiles.inferColumnTypes=false` and `schemaEvolutionMode=rescue`.

**Why:** a single bad row from upstream can silently change a column type and corrupt every downstream join. Pinning makes drift loud (`_rescued_data` becomes non-empty) instead of silent. Bumping `SCHEMA_VERSION` is a coordinated breaking change with the producer — explicit, auditable.

### 3.4 Idempotent transforms
Every Silver and Gold job re-runs safely:
- `silver.events` — Delta `MERGE` on `event_id`.
- SCD2 dimensions — hash-driven boundary detection + `overwrite`.
- `silver.fact_orders` — `overwrite` partitioned by `order_date`.
- Gold marts — `overwrite` with `overwriteSchema=true`.

**Why:** retries, backfills, and partial reprocessing are normal in data engineering. If re-running a job means deduping its output by hand, the job is not finished.

### 3.5 Notebooks as thin shells
Every notebook is widgets + imports + module call. All transformation logic lives in `src/`.

**Why:** notebooks are not unit-testable, not version-control-friendly when full of inline code, and tempt copy-paste duplication. Pushing logic into `src/` makes it testable on a developer laptop, reviewable in a PR diff, and reusable across notebooks and future Asset Bundles.

### 3.6 Config-driven, environment-portable
All catalog/schema/volume/trigger names live in `conf/pipeline.yml`. Override any leaf with an env var: `ECOM_PIPELINE_<DOTTED_KEY>=value`.

**Why:** the same code runs in dev and prod by setting `ECOM_PIPELINE_CATALOG=dev_main`. No per-environment forks; no manual edits before promoting.

### 3.7 Cost-aware streaming
Default streaming trigger is `availableNow` — process new files and exit.

**Why:** on Databricks Free Edition, keeping a stream alive 24/7 wastes the shared cluster. `availableNow` makes ingestion behave like a finite job that fits a Workflow schedule and lets the cluster auto-terminate. One config flip switches to continuous (`processingTime=30 seconds`) when production needs it.

---

## 4 · Tradeoffs and engineering reasoning

### 4.1 `MERGE` for facts, `overwrite` for marts
Silver `events` is large enough that incremental `MERGE` pays off. SCD2 dimensions and Gold marts are small/regenerable; `overwrite` is simpler and avoids `MERGE`'s concurrency pitfalls.

**Tradeoff:** `MERGE` is more complex and slightly slower per row, but bounded write volume. `overwrite` is fast and brutally simple, but rewrites the whole table. We picked the right tool per case rather than forcing a single pattern.

### 4.2 Re-derive `session_id_silver` instead of trusting the producer
The producer emits its own `session_id`. Silver discards it (preserved as `session_id_raw` for debugging) and re-derives `session_id_silver` from event-time gaps using `Window` + `lag` + cumulative `sum`.

**Why:** in a real Kafka topology, the producer's session ID may not survive shuffles across shards, and late events would be assigned to the wrong session. A time-gap-derived ID is deterministic from `event_ts` alone and shuffle-safe.

**Tradeoff:** the new ID is computed at Silver, not at the source — there's a one-stage delay before sessions are visible. We accept that for correctness.

### 4.3 SCD2 by attribute hash, not raw equality
We hash only the **tracked** columns (e.g. `email, country, city, marketing_opt_in, loyalty_tier`) — `updated_ts` is excluded. New SCD2 versions are opened only when the hash changes.

**Why:** `updated_ts` advances on every snapshot even when the payload is identical. Hashing it would open a spurious version per snapshot, bloating the dimension and corrupting point-in-time joins. Excluding it makes the transform robust to noisy upstream timestamps.

### 4.4 ZORDER never on partition columns
ZORDER targets are declared in `conf/pipeline.yml`. Partition columns are intentionally absent.

**Why:** Delta raises `DELTA_ZORDERING_ON_PARTITION_COLUMN` (SQLSTATE 42P10). Partitioning already provides the same data-skipping benefit. ZORDER targets are the columns BI predicates actually filter on (`category`, `country`, `event_ts`, `user_id`).

### 4.5 Pure-Python tests in CI; PySpark optional
Tests run with `pip install -e .[dev]` (no PySpark — ~300 MB saved per CI cell). PySpark belongs in an opt-in `[spark]` extra; the `spark` pytest fixture skips cleanly when the package is missing.

**Tradeoff:** integration tests against a real Spark session must live in a dedicated job that installs `[spark]`. We accept the second job for the speed and reliability of the default lane.

### 4.6 Append-only Bronze + idempotent Silver beats audit columns
We don't add `created_at`/`updated_at` columns to Silver tables. The Delta time-travel log, plus the immutable Bronze replay log, gives full auditability without column-level tracking.

**Why:** audit columns drift from the truth (people forget to update them). Delta history doesn't lie — `DESCRIBE HISTORY <table>` is the source of truth.

### 4.7 Thin shell notebooks > Databricks-specific Python
Notebooks contain only `dbutils.widgets`, `dbutils.fs` calls, and imports from `src/`. The `src/` modules are pure Python or PySpark — never `dbutils`-bound.

**Why:** code that imports `dbutils` cannot run locally, cannot be unit-tested, and cannot be reused outside the workspace. Confining `dbutils` to notebook cells keeps the orchestration concerns at the edge.

---

## 5 · Cross-cutting concerns

| Concern | Where it lives | Approach |
|---|---|---|
| Schema | `src/common/schemas.py`, `src/common/version.py` | Pinned `StructType`s + `SCHEMA_VERSION` constant |
| Config | `conf/pipeline.yml`, `conf/simulator.yml`, `src/common/config.py` | YAML + env override |
| IO | `src/common/io.py` | `TableRef`, `upsert/overwrite/optimize`, volume paths |
| Quality | `src/common/quality.py`, `notebooks/99_quality_checks.py` | `Expectation` predicates; `enforce()` raises on `fail` |
| CI/CD | `.github/workflows/ci.yml`, `pyproject.toml` | Lint + import smoke + pytest + config validation + gitleaks |
| Tests | `tests/` | Pure-Python; optional `spark` fixture |

---

## 6 · Production substitution map

The architectural shape — medallion, event-first, decoupled producer — stays identical when promoted to production. Only the substrate changes.

| Concern         | This project (Free Edition)   | Production                                      |
|-----------------|-------------------------------|-------------------------------------------------|
| Ingestion       | Auto Loader on Volume         | Kafka / Kinesis / Event Hubs structured streaming |
| Compute         | Single shared cluster         | Per-layer job clusters · Photon · spot           |
| Orchestration   | Databricks Workflows          | Workflows + Delta Live Tables                   |
| Performance     | Manual `OPTIMIZE` + Z-ORDER   | Liquid Clustering · Predictive Optimization      |
| Quality         | `Expectation` helper          | DLT expectations or Great Expectations in CI    |
| Governance      | Single UC                     | Full UC: lineage, ABAC, column masks, tagging   |
| Schema          | `_rescued_data` column        | Confluent / Apicurio Schema Registry            |
| CI/CD           | GitHub Actions                | + Databricks Asset Bundles for declarative deploy |
| ML              | Notebook training             | Feature Store + MLflow + Model Serving          |

---

## 7 · DLT Pipeline (Alternative Execution Path)

> Full end-to-end reference for the DLT pipeline lives in [`dlt_pipeline.md`](./dlt_pipeline.md) (code structure, execution model, deployment, anti-patterns). This section is the architectural summary.

The same medallion logic also ships as a **Delta Live Tables** pipeline under [`pipelines/dlt/`](../pipelines/dlt/). The DLT pipeline is additive — the Workflow job described in §1–6 is unchanged and remains the notebook-based path. Both paths are now **fully functional and verified end-to-end** in the dev workspace.

The DLT pipeline is **self-contained**: it does **not** import from `src/`. The three source schemas (`EVENT_SCHEMA`, `USER_SCHEMA`, `PRODUCT_SCHEMA`) are duplicated at `pipelines/dlt/schemas.py` in lockstep with `src/common/schemas.py`. The two paths share the **landing Volume** (Bronze input) but write to **different output schemas**, so they cannot contend on the same Delta tables.

### 7.1 Two execution paths over the same logic

```
                       ┌──────────────────────────┐
                       │  Landing Volume (shared) │
                       │   /Volumes/<cat>/        │
                       │   <bronze_schema>/       │
                       │   landing/{events,...}   │
                       └────────────┬─────────────┘
                                    │
                ┌───────────────────┴────────────────────┐
                ▼                                        ▼
   ┌────────────────────────┐                ┌────────────────────────┐
   │ Workflow `medallion`   │                │ DLT pipeline           │
   │                        │                │   `bricksshop-dlt`     │
   │  notebooks/00 … 99     │                │  pipelines/dlt/*.py    │
   │  imperative PySpark    │                │  declarative @dlt.table│
   │  Auto Loader + MERGE   │                │  Auto Loader (DLT-     │
   │  + manual checkpoints  │                │  managed checkpoints)  │
   │  imports from src/     │                │  serverless: true      │
   │                        │                │  self-contained — no   │
   │                        │                │  import from src/      │
   └────────────┬───────────┘                └────────────┬───────────┘
                ▼                                         ▼
   ${catalog}.ecom_bronze / silver / gold        ${catalog}.${dlt_schema}
   (events_raw, events, fact_orders, …)          (same table names, one schema)
```

The DLT pipeline targets a **single schema** (`${var.dlt_schema}`, default `ecom_dlt`) for all bronze / silver / gold tables. Table names match the Workflow path (`events_raw`, `events`, `fact_daily_sales`, …) so query templates stay portable between the two — only the schema differs.

### 7.2 What the DLT path replaces

| Workflow piece                           | DLT equivalent                                   |
|------------------------------------------|--------------------------------------------------|
| `notebooks/20_bronze.py` + Auto Loader   | `pipelines/dlt/bronze.py` (`@dlt.table`, streaming) |
| `notebooks/30_silver.py` + `src/silver`  | `pipelines/dlt/silver.py` (materialised views)   |
| `notebooks/40_gold.py` + `src/gold`      | `pipelines/dlt/gold.py` (materialised views)     |
| `notebooks/99_quality_checks.py`         | `@dlt.expect` / `@dlt.expect_or_fail` decorators |
| Hand-managed `_checkpoints/<source>/`    | DLT-managed checkpoints (opaque to the user)     |
| `OPTIMIZE … ZORDER BY` calls             | Predictive Optimization (when enabled at workspace)|

### 7.3 Tradeoffs

| Dimension           | Workflow job                              | DLT pipeline                                   |
|---------------------|-------------------------------------------|------------------------------------------------|
| **Cost**            | Reuses the shared cluster on Free Edition | Runs on serverless DLT compute (mandatory on Free Edition) |
| **Simplicity**      | Plain notebooks, plain Spark APIs         | One DLT decorator per output table             |
| **Observability**   | Workflow run page; cell output            | DLT event log; per-expectation drop counters   |
| **Lineage**         | Implicit (notebook order)                 | Explicit DAG inferred from `dlt.read(...)`     |
| **Idempotency**     | Hand-coded MERGE / overwrite              | Built-in: tables are rebuilt or appended       |
| **Scalability**     | Manual cluster sizing                     | Serverless — sizing is implicit                |
| **Schema evolution**| Auto Loader rescue mode                   | Same, plus DLT-tracked schema versions         |
| **Code reuse**      | Imports from `src/` shared with tests     | Self-contained under `pipelines/dlt/` (DLT runtime cannot import `src/`) |
| **When to pick it** | Default daily operation on Free Edition; richer test coverage | Demo of declarative ops; richer observability; production migration path |

The two paths intentionally cover the same logic with different ergonomic / cost profiles. They are not staged — pick one per environment based on the SLA and budget.

### 7.4 Bundle wiring

Both resources live in [`databricks.yml`](../databricks.yml). The DLT pipeline (`ecom_dlt_pipeline`) is declared at the bundle root with `serverless: true`, `development: true`, `continuous: false`, and **no cluster configuration** — Free Edition mandates serverless compute for DLT. It is shipped with four library files in this order: `schemas.py`, `bronze.py`, `silver.py`, `gold.py`. The Workflow job (`medallion`) is declared inside the `dev` and `prod` targets via a YAML anchor so a third target — `dev_dlt_only` — can deploy the DLT pipeline alone.

CD currently auto-runs **only the DLT pipeline** on push to `main`; the medallion job is deployed but must be triggered manually (see [`ci_cd.md`](./ci_cd.md) §8.7 / 8.8 and [`runbook.md`](./runbook.md) §1.2).

---

## 8 · Chat stack on top of the lakehouse

The same FastAPI process that ingests events also hosts a **multi-agent chat assistant**. It is read-only against the catalog (no writes to Silver / Gold tables); the only writes it produces — `add_to_cart` events from the chat widget — flow back through the standard `/events/batch` path so the lakehouse pipeline picks them up like any other event source.

```
user message
     │
     ▼
┌───────────────┐    intent + clean query + filters
│ Router LLM    │────────────────────────────────────►
└───────────────┘                                     │
                                                      ▼
                            ┌──────────────────────────────────────┐
                            │ ProductCatalog.semantic_search       │
                            │   Tier 1 — Vector Search             │
                            │   Tier 2 — FAISS index in Volume     │  ◄── currently serves traffic
                            │   Tier 3 — SQL ILIKE (last resort)   │
                            └─────────────────┬────────────────────┘
                                              │ scored candidates
                                              ▼
                            ┌──────────────────────────────────────┐
                            │ HybridRanker                         │
                            │   final = 0.60·semantic              │
                            │         + 0.15·price                 │
                            │         + 0.25·popularity            │
                            └─────────────────┬────────────────────┘
                                              ▼
                                       ┌────────────────┐
                                       │ Composer LLM   │  natural-language reply
                                       └────────────────┘
```

### 8.1 Why FAISS, not Vector Search

Free Edition workspaces do not expose Databricks Vector Search endpoints. We kept the Tier-1 integration in code (`web/backend/vector_search.py`, `pipelines/vector_search/setup.py`) so the moment an endpoint becomes available the chat stack picks it up via env vars (`VS_ENDPOINT`, `VS_INDEX_NAME`) — no code change. In the meantime, **FAISS is the live tier**, built as a production-ready fallback rather than a placeholder:

* embeddings table (`<catalog>.ecom_dlt.product_embeddings`) carries `embedding_model_name` + `embedding_version` per row for full SQL-side auditability,
* a versioned `faiss_index_v{ts}.idx` lives on a Unity Catalog Volume; a pointer file (`faiss_index_latest.txt`) is written **last** by every rebuild, so partial runs never poison readers,
* the backend polls the pointer and **hot-reloads** atomically — no FastAPI restart between rebuilds.

Full deep dive: [`search.md`](./search.md).

### 8.2 Why hybrid ranking

Cosine similarity alone surfaces text-matchy products that may be priced badly or unbought. The ranker layers two business signals on top:

* **Price.** Inverse min-max within the candidate pool (cheapest = 1.0, priciest = 0.0). Per-query, so a $5,000-sofa search and a $20-toaster search both get a full-range price contribution.
* **Popularity.** Polled in-memory cache of `purchases_7d` + `add_to_cart_7d` + `views_7d` per product, log-normalised against the catalog max. Source SQL aggregates `fact_orders` (exploded `items`) and `events` in one roundtrip.

Weights default to `0.60 / 0.15 / 0.25` (semantic / price / popularity), env-overridable. Cold-start safe: when the popularity cache is empty, the ranker emits 0.0 for the popularity component and the score reduces to `w_s·sem + w_p·price` — still strictly more useful than pure semantic. Full deep dive: [`ranking.md`](./ranking.md).

### 8.3 Bundle wiring

The chat stack adds one job to [`databricks.yml`](../databricks.yml):

| Job | What it does | Schedule |
|---|---|---|
| `bricksshop_faiss_rebuild` | Re-encodes `dim_products_scd2`, refreshes `product_embeddings`, writes a new `.idx` + pointer | Daily 03:30 UTC, paused by default |

The hybrid-ranker popularity cache is **not** a separate job — it's a daemon thread inside the FastAPI process that runs the aggregation SQL every `POPULARITY_REFRESH_INTERVAL_S` (default 900s). Decision: a new Delta table + scheduled job felt heavier than the problem (one warehouse query per 15 min for the whole catalog). When this stack moves to a multi-instance deploy we'll promote it to a shared Delta table.

### 8.4 Orchestrator safeguards (execution control + safety)

The chat stack reads from `dim_products_scd2`, `events`, and `fact_orders`. Those tables are the orchestrator's responsibility, and the orchestrator (`ecom_orchestrator`) is parameterised by `mode ∈ {prod, simulator, full}`. Two design choices around `mode` are worth calling out separately because they're **execution-control strategy** for the pipeline and a **safety mechanism** for production data.

**Always-run simulator (execution control).** The simulator notebook (`notebooks/11_simulate.py`) is wired into the orchestrator DAG **unconditionally** — it's not gated by a Databricks `condition_task` upstream. Reason: Databricks Jobs ignores the `outcome:` qualifier under `run_if: ALL_DONE` / `NONE_FAILED`; only `ALL_SUCCESS` honours `outcome:`, but `ALL_SUCCESS` blocks any downstream task whose upstream is `EXCLUDED`. Keeping the simulator always-run lets us use `ALL_SUCCESS` end-to-end and still get correct mode semantics across `prod` / `simulator` / `full`. This is execution-control glue around a real platform constraint, not laziness.

**Prod-mode short-circuit (safety).** Because the simulator always runs, the **simulator notebook itself** reads the `mode` widget and exits as a no-op (`dbutils.notebook.exit("skipped: mode=prod")`) when `mode=prod`. This is a deliberate safety mechanism: production runs must **never** generate synthetic events into the landing zone. The check sits at the top of the notebook, before any RNG seed or volume write — there's no code path through which `mode=prod` produces data. The chat stack's downstream tables (and therefore its hybrid ranker's popularity signal) are protected from synthetic contamination by construction.

| Concern | Mechanism | Where it lives |
|---|---|---|
| Cannot use `condition_task` to exclude the simulator without breaking DLT downstream | Always-run simulator + `ALL_SUCCESS` chain | `databricks.yml` (orchestrator job) |
| Production must never produce synthetic events | Notebook short-circuits when `mode=prod` | `notebooks/11_simulate.py` (top of file) |
| DLT must run in `prod` and `full`, must skip in `simulator` | Single `condition_task` (`gate_dlt`) on `mode != simulator` + default `ALL_SUCCESS` | `databricks.yml` (orchestrator job) |

The result: one `mode` parameter at the top of the bundle controls all three execution shapes, and the safety property *("`prod` never writes synthetic data")* is enforced at the **innermost layer** rather than relying on the outer DAG gating to be configured correctly. If someone removes the gate by accident, `mode=prod` is still safe.

---

## 9 · Where to go next

- Reading **for runtime behaviour** → [`bronze_layer.md`](./bronze_layer.md), [`silver_layer.md`](./silver_layer.md), [`gold_layer.md`](./gold_layer.md)
- Reading **for the data shape** → [`data_model.md`](./data_model.md)
- Reading **for operability** → [`runbook.md`](./runbook.md), [`ci_cd.md`](./ci_cd.md), [`data_quality.md`](./data_quality.md)
- Reading **for the chat stack** → [`architecture_overview.md`](./architecture_overview.md), [`search.md`](./search.md), [`ranking.md`](./ranking.md)
