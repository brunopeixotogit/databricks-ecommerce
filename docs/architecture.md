# Architecture

End-to-end design of the e-commerce lakehouse on **Databricks + Delta Lake**, organised as a Bronze → Silver → Gold medallion. This document explains the system shape, the data flow, and — most importantly — the *reasoning* behind each structural choice.

> Companion docs: [`bronze_layer.md`](./bronze_layer.md), [`silver_layer.md`](./silver_layer.md), [`gold_layer.md`](./gold_layer.md), [`data_model.md`](./data_model.md), [`data_quality.md`](./data_quality.md), [`ci_cd.md`](./ci_cd.md), [`runbook.md`](./runbook.md).

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

## 7 · Where to go next

- Reading **for runtime behaviour** → [`bronze_layer.md`](./bronze_layer.md), [`silver_layer.md`](./silver_layer.md), [`gold_layer.md`](./gold_layer.md)
- Reading **for the data shape** → [`data_model.md`](./data_model.md)
- Reading **for operability** → [`runbook.md`](./runbook.md), [`ci_cd.md`](./ci_cd.md), [`data_quality.md`](./data_quality.md)
