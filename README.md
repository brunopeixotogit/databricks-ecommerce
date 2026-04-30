# E-commerce Data Platform on Databricks

A production-style **lakehouse pipeline** that simulates an e-commerce site (electronics, appliances, furniture), ingests clickstream and snapshot data through **Auto Loader**, and refines it across a **Bronze → Silver → Gold** medallion architecture on **Delta Lake** — ending in four analytics-ready marts: daily sales, conversion funnel, abandoned carts, and a per-user 360.

> Designed and tuned to run end-to-end on **Databricks Free Edition**, with a deliberately substrate-agnostic design: replacing the synthetic producer with Kafka or Kinesis only changes the ingestion connector — every downstream module stays identical.

![Stack](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD8?logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![Tests](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)

---

## Why this project

Data-engineering portfolios tend to stop at "I can read a CSV with Spark." This one shows the moving parts of a real lakehouse:

- a **synthetic-but-realistic producer** with a session state machine, weighted devices, and tunable funnel probabilities;
- a **streaming ingestion layer** with pinned schemas, rescued-data handling, and durable checkpoints;
- **idempotent transforms** — Delta `MERGE` for events, attribute-hash SCD2 for dimensions, business-rule order rollups;
- **query-tuned Gold marts** with `OPTIMIZE` / `ZORDER BY`, partitioned for predicate pushdown;
- **operational hygiene** — config + env overrides, quality expectations, unit tests, Workflow-friendly triggers.

The full technical write-up is split into focused documents under [`docs/`](./docs/README.md):

| Doc | Read this for |
|---|---|
| [`docs/architecture.md`](./docs/architecture.md)     | End-to-end design, data flow, principles, tradeoffs |
| [`docs/bronze_layer.md`](./docs/bronze_layer.md)     | Auto Loader, schema enforcement, append-only contract |
| [`docs/silver_layer.md`](./docs/silver_layer.md)     | Dedup, sessionisation, SCD2 by attribute hash |
| [`docs/gold_layer.md`](./docs/gold_layer.md)         | Marts, KPIs, BI readiness |
| [`docs/data_model.md`](./docs/data_model.md)         | Pinned schemas, ER relationships |
| [`docs/data_quality.md`](./docs/data_quality.md)     | Expectation framework, severity, failure handling |
| [`docs/ci_cd.md`](./docs/ci_cd.md)                   | GitHub Actions, Databricks Asset Bundles, deploy plan |
| [`docs/runbook.md`](./docs/runbook.md)               | Step-by-step run, debugging, reprocessing |

> **Two execution paths.** The medallion logic ships as both a Workflow-based job (the canonical path) and a Delta Live Tables pipeline (`pipelines/dlt/`). See **[DLT Pipeline (Alternative Execution Path)](#dlt-pipeline-alternative-execution-path)** below.

---

## Architecture

```
                    ┌──────────────────────────┐
                    │      Simulator           │
                    │  (Python · faker · RNG)  │
                    └────────────┬─────────────┘
                                 │  gzipped JSON, partitioned by dt=YYYY-MM-DD
                                 ▼
   /Volumes/main/ecom_bronze/landing/{events, users, products}/
                                 │
                                 │  Auto Loader  (cloudFiles · schemaEvolutionMode=rescue)
                                 ▼
   ┌──────────────── BRONZE ────────────────┐    append-only · partition by _ingest_date
   │ events_raw     users_raw    products_raw│
   └────────────────────┬───────────────────┘
                        │  PySpark · Delta MERGE · Window functions
                        ▼
   ┌──────────────── SILVER ────────────────┐    dedup · sessionize · SCD2
   │ events     dim_users_scd2              │
   │ fact_orders  dim_products_scd2         │
   └────────────────────┬───────────────────┘
                        │  groupBy / agg / explode · OPTIMIZE ZORDER
                        ▼
   ┌──────────────── GOLD ──────────────────┐    BI / CRM / ML feature ready
   │ fact_daily_sales      fact_funnel      │
   │ fact_abandoned_carts  dim_user_360     │
   └────────────────────────────────────────┘
```

**Layer contracts**

| Layer | Contract | Idempotency mechanism |
|---|---|---|
| Bronze | Source-faithful, append-only, no business rules | Auto Loader checkpoint + `_rescued_data` |
| Silver | Cleansed, deduped, conformed, sessionised, SCD2 | Delta `MERGE` on `event_id`; SCD2 by attribute hash |
| Gold  | Denormalised marts optimised for query speed     | Full `overwrite` (`overwriteSchema=true`) |

---

## Highlight features

### Realistic event simulator
A session state machine — `LANDING → BROWSE → (CART → (CHECKOUT → PURCHASE | ABANDON)| EXIT)` — with weighted device mix (62 % mobile / 30 % desktop / 8 % tablet), Poisson-distributed page views, and per-category price ranges. **Deterministic for the same seed**, so backfills are reproducible.

### Pinned schemas, no `inferSchema`
Every source has a `StructType` in `src/common/schemas.py` plus a `SCHEMA_VERSION` constant. Unknown fields land in `_rescued_data` instead of breaking the stream. Type drift becomes loud, not silent.

### True idempotency
- **Silver events** — deduped by `row_number()` over `(event_id ORDER BY _ingest_ts DESC)`, written via Delta `MERGE` on `event_id`.
- **SCD2 dimensions** — boundaries are detected by hashing only the *tracked* columns (`updated_ts` is intentionally excluded), so re-emitting an unchanged snapshot does not open a spurious new version.
- **Auto Loader** — durable checkpoints under `_checkpoints/<source>` make re-runs free.

### Sessionisation that survives shuffles
`session_id_silver = <user_id|anon>_<seq>` is **re-derived** from event-time gaps (`> 30 min`) using `Window` + `lag` + cumulative `sum`. The producer's `session_id` is preserved as `session_id_raw` for debugging — but the canonical key works correctly even when events arrive out of order across shards.

### Cost-aware streaming
Default trigger is `availableNow`: process new files and exit. On Free Edition this prevents a stream from holding the cluster 24/7. A single env/widget flip switches to continuous (`processingTime=30 seconds`).

### Config-driven, environment-portable
All catalog / schema / volume / trigger / ZORDER targets live in `conf/pipeline.yml`. Override any leaf with an env var:
```bash
export ECOM_PIPELINE_CATALOG=dev_main          # routes the whole pipeline to a dev catalog
export ECOM_SIMULATOR_USERS_INITIAL_POPULATION=100   # tiny smoke run
```

### Notebooks are thin shells
Every notebook is `widgets + imports + module call`. Real logic lives in `src/`, where it is unit-testable without a cluster.

---

## Tech stack

- **Databricks** — Unity Catalog, Volumes, Workflows.
- **Delta Lake** — ACID, time travel, `MERGE`, `OPTIMIZE`/`ZORDER BY`, schema evolution.
- **PySpark** — DataFrame API; `Window` for dedup / sessionise / SCD2; Delta Python bindings for `MERGE`.
- **Auto Loader (`cloudFiles`)** — incremental file discovery, schema location, rescue mode.
- **Faker · PyYAML · pytest · chispa**.

---

## Repository layout

```
databricks_ecommerce/
├── conf/                       # pipeline + simulator config (env-overridable)
├── notebooks/                  # 00_setup → 10_simulator → 20_bronze → 30_silver → 40_gold → 99_quality
├── src/
│   ├── common/                 # TableRef, IO helpers, schemas, config loader, quality
│   ├── simulator/              # entity & behavior generators, JSON emitter
│   ├── bronze/                 # Auto Loader streams (events + snapshots)
│   ├── silver/                 # dedup + sessionise, SCD2, fact_orders
│   └── gold/                   # daily_sales, funnel, abandoned_carts, user_360
├── tests/                      # pytest + chispa unit tests
├── documentation.md            # full technical write-up
├── requirements.txt
└── README.md
```

---

## Quick start

### On Databricks
1. **Import** this repo via *Workspace → Repos → Add Repo*.
2. Run `notebooks/00_setup.py` (creates the catalog if missing, schemas, and the `landing` Volume with its subdirectories).
3. Run `notebooks/10_run_simulator.py` (installs `faker`, restarts Python, emits users + products + events).
4. Run the medallion in order: `20_bronze.py` → `30_silver.py` → `40_gold.py`.
5. Run `notebooks/99_quality_checks.py` to validate the layer.
6. *(Optional)* Wire the six notebooks into a **Databricks Workflow** for continuous operation.

All catalog / schema / volume names are widget parameters, so the same code runs in dev and prod with no edits.

### Locally (tests only)
```bash
pip install -r requirements.txt
pytest tests/
```
The simulator and quality helpers are pure Python — most logic can be tested without a Databricks cluster.

---

## CI/CD pipeline (current state)

Two GitHub Actions workflows protect and promote `main`. Both are **active and run automatically**.

### CI runs on every PR and push to `main`

`.github/workflows/ci.yml`:

- **Ruff linting** on `src/` and `tests/`
- **Unit tests** with `pytest --cov=src` on a matrix of **Python 3.10 / 3.11 / 3.12**
- **Import-graph smoke** — every pure-Python module under `src/` must import without PySpark
- **Config validation** — every `conf/*.yml` is loaded through `src.common.config.load_config`
- **Secret scanning** — Gitleaks across the full git history (advisory)

### CD runs on push to `main` — **only when pipeline-relevant paths change**

`.github/workflows/cd.yml`:

- Installs the Databricks CLI (`databricks/setup-cli@main`)
- `databricks bundle validate --target dev`
- `databricks bundle deploy --target dev` (uploads notebooks, updates Job definition)
- `databricks bundle run --target dev medallion` (executes the full Bronze → Silver → Gold DAG)

**Path filter on the `push` trigger.** CD evaluates whether at least one of the following paths changed in the commit; if not, the workflow is skipped and **no Databricks compute is consumed**:

```
src/**
notebooks/**
databricks.yml
conf/**
.github/workflows/**
```

A docs-only commit (`README.md`, `docs/**`, any `*.md`) merges to `main`, runs CI for code-health verification, and **does not** trigger a Databricks deploy or job run. `workflow_dispatch` (manual runs) has no path filter — the **Run workflow** button always reruns CD.

### End-to-end flow

```
git push origin main
        │
        ▼
┌──────────────── CI (ci.yml) ────────────────┐
│  ruff · pytest matrix · import smoke ·       │
│  conf validation · gitleaks                  │
└──────────────────────┬───────────────────────┘
                       │ green
                       ▼
┌──────────────── CD (cd.yml) ────────────────┐
│  bundle validate → deploy → run medallion    │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌────────── Databricks workspace ─────────────┐
│  setup → create_tables → simulate →          │
│  bronze → silver → gold → quality            │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
            Gold tables refreshed
        (fact_daily_sales, fact_funnel,
         fact_abandoned_carts, dim_user_360)
```

### Current architecture state

Three independent layers, all live and consistent with the code:

| Layer | What's running | Where it's defined |
|---|---|---|
| **Data pipeline (Bronze → Silver → Gold)** | 7-task DAG: `00_setup` → `01_create_tables` → `10_run_simulator` → `20_bronze` → `30_silver` → `40_gold` → `99_quality_checks`. Idempotent end-to-end (Auto Loader checkpoints, Delta `MERGE` on `event_id`, hash-driven SCD2, full-rebuild Gold). | `notebooks/`, `src/{bronze,silver,gold,common}/` |
| **Orchestration — Databricks Asset Bundle** | One Workflow `[dev bru_peixoto] ecom-medallion` declared in `databricks.yml`. Two targets (`dev` / `prod`). Schedule **PAUSED** by design; runs are triggered by CD. | `databricks.yml` |
| **CI/CD — GitHub Actions** | `ci.yml` (lint + matrix tests + config validation + secrets scan) on every PR/push; `cd.yml` (bundle validate / deploy / run) on push to `main`, **path-filtered to pipeline-relevant changes**. | `.github/workflows/` |

Coverage scope is intentionally limited to pure-Python modules (`fail_under = 60 %`, currently 79 %). PySpark-bound modules are exercised by Databricks at deploy time, not by the default CI lane.

### Deployment rules

The single source of truth for "when does anything run":

| Event | CI runs? | CD runs? | Databricks deploy? |
|---|---|---|---|
| PR opened / updated against `main` | ✅ | ❌ | ❌ |
| Push to `main` touching `src/**`, `notebooks/**`, `databricks.yml`, `conf/**`, or `.github/workflows/**` | ✅ | ✅ | ✅ |
| Push to `main` touching **only** `*.md` / `docs/**` / other non-pipeline files | ✅ | ❌ skipped by path filter | ❌ |
| Manual **Run workflow** on `cd.yml` (workflow_dispatch) | n/a | ✅ always | ✅ |
| Push to a non-`main` branch | ❌ | ❌ | ❌ |
| Tag push (`v*.*.*`) — prod release | ❌ | ❌ (prod CD intentionally not wired yet) | ❌ |

**What prevents unnecessary executions:**
- Docs-only commits are skipped at the workflow level via `on.push.paths` in `cd.yml` — no runner spins up, no Databricks compute is consumed.
- `concurrency.group: cd-dev` with `cancel-in-progress: false` serialises deploys without killing them mid-flight.
- The Workflow schedule in `databricks.yml` is **PAUSED** — runs only happen when CD (or a human) triggers them, never on a hidden cron.
- `99_quality_checks` is the final task: a `severity="fail"` predicate violation aborts the run so stale data never gets blessed as "deployed."

### Required GitHub Secrets

Configured in **Repo → Settings → Environments → `dev`** (scoped, audited):

| Secret | What it is |
|---|---|
| `DATABRICKS_HOST_DEV`  | dev workspace URL (e.g. `https://dbc-xxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN_DEV` | OAuth M2M token for a service principal scoped to dev |

The deploy uses the bundle's `dev` target (`catalog: dev_main`); the `prod` target is never touched by automation.

> Production deploys are intentionally **tag-gated and not yet wired** — see [`docs/ci_cd.md § 8.4`](./docs/ci_cd.md). Dev CD is the canonical promotion path today.

---

## Testing strategy

Three layers of tests, each catching a different class of failure.

### 1 · CI tests (code level)

Run by `.github/workflows/ci.yml` on every PR and push.

| Check | Tool | Catches |
|---|---|---|
| Linting          | Ruff (`E`, `F`, `I`, `B`, `UP`, `SIM`) | Style drift, dead imports, deprecated typing forms |
| Unit tests       | pytest 8 + coverage (≥ 60 %)           | Behavioural regressions in pure-Python logic |
| Multi-version    | matrix `3.10 / 3.11 / 3.12`            | Version-specific bugs (PEP 604 unions, dataclass kw-only, typing.Self) |
| Import-graph     | inline Python script                   | An accidental `from pyspark.sql import …` in a pure-Python module |
| Config schema    | `load_config("pipeline" / "simulator")` | Malformed YAML; missing keys |
| Secrets          | Gitleaks                               | Accidentally-committed tokens / `.env` / `*.pem` |

**Concrete failure scenario.** A contributor edits `src/silver/events.py` and breaks the dedup window — say `Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())` becomes `.orderBy("_ingest_ts")` (ascending). Their PR runs through CI:

1. Ruff passes (style is fine).
2. The pure-Python tests pass (Silver isn't directly tested with a real DataFrame in this lane).
3. **Import-graph smoke** still passes — but if they accidentally added `from pyspark.sql import …` to a pure-Python module to debug, **all three matrix cells fail**.
4. After merge, **CD runs Silver against the dev workspace**, and `99_quality_checks` raises — see § 2 below.

### 2 · Data quality tests (pipeline level)

Run inside Databricks by `notebooks/99_quality_checks.py`. Every expectation is a SQL predicate that must evaluate `TRUE` for a valid row. `severity="fail"` violations raise `AssertionError` and abort the Workflow run.

| Validation target | Predicate | Why it exists |
|---|---|---|
| `event_id` integrity | `event_id IS NOT NULL` (fail) | Dedup keys on `event_id`; null = uniqueness cannot be guaranteed |
| Event-type domain | `event_type IN ('page_view','add_to_cart','purchase','abandon_cart')` (fail) | Producer drift would break every downstream transformation |
| Sessionisation invariant | `session_id_silver IS NOT NULL` (fail) | Funnel and abandoned-cart marts depend on it |
| Purchase wholeness | `event_type <> 'purchase' OR order_id IS NOT NULL` (fail) | A purchase without `order_id` is silently lost in `fact_orders` groupby |
| Price sanity | `price IS NULL OR price >= 0` (warn) | Surfaces upstream bugs without blocking |
| Order totals | `total >= 0`, `subtotal >= 0`, `size(items) > 0` (fail) | `fact_daily_sales` would be wrong if violated |
| Total math | `abs(total - (subtotal + tax + shipping)) < 0.05` (warn) | Detects drift between Silver math and any future rule change |

The full framework is documented in [`docs/data_quality.md`](./docs/data_quality.md).

### 3 · SQL validation tests (real queries)

These can be run by hand against the dev workspace after any deploy, or wired into `99_quality_checks` for permanent monitoring.

#### Bronze ↔ Silver consistency
```sql
SELECT COUNT(*) AS bronze_rows FROM dev_main.ecom_bronze.events_raw;
SELECT COUNT(*) AS silver_rows FROM dev_main.ecom_silver.events;
```
Silver should be **≤ Bronze** (filter + dedup) but typically within ~5 %. A larger gap signals dedup or filtering misbehaviour.

#### Event-type integrity
```sql
SELECT event_type, COUNT(*) AS rows
FROM   dev_main.ecom_silver.events
GROUP  BY event_type
ORDER  BY rows DESC;
```
Expect exactly four rows: `page_view`, `add_to_cart`, `purchase`, `abandon_cart`. Anything else means producer drift slipped through Bronze rescue mode.

#### Deduplication (no `event_id` may appear twice in Silver)
```sql
SELECT event_id, COUNT(*) AS dup_count
FROM   dev_main.ecom_silver.events
GROUP  BY event_id
HAVING COUNT(*) > 1;
```
Should return **zero rows**. A row here means the `MERGE` on `event_id` failed or the dedup window changed.

#### SCD2 correctness (each user has exactly one `is_current = true` row)
```sql
SELECT user_id, COUNT(*) AS current_versions
FROM   dev_main.ecom_silver.dim_users_scd2
WHERE  is_current = true
GROUP  BY user_id
HAVING COUNT(*) > 1;
```
Should return **zero rows**. More than one current version means the hash-boundary detection broke or `valid_to` filling regressed.

#### Fact-orders integrity
```sql
SELECT COUNT(*)             AS orders,
       ROUND(SUM(total), 2) AS total_gmv_with_tax_shipping,
       ROUND(SUM(subtotal + tax + shipping), 2) AS recomputed_total
FROM   dev_main.ecom_silver.fact_orders;
```
`total_gmv_with_tax_shipping` should equal `recomputed_total` to within rounding (< $0.01 per order). Drift means the tax/shipping rule changed without updating the column.

#### Bronze → Gold reconciliation
```sql
SELECT (SELECT SUM(item.line_total)
        FROM dev_main.ecom_silver.fact_orders LATERAL VIEW EXPLODE(items) AS item)  AS silver_gmv,
       (SELECT SUM(gmv) FROM dev_main.ecom_gold.fact_daily_sales)                    AS gold_gmv;
```
Two values should match exactly. Mismatch means Gold is reading stale Silver or vice versa.

### 4 · Why each test layer exists

| Layer | Production failure simulated |
|---|---|
| **CI tests** | Refactor breaks behaviour; a typo in a typing import; secrets committed by mistake. Catches the **developer-side** failure modes before any data ever moves. |
| **Data quality tests** | Producer drift; Kafka at-least-once duplication; late-arriving events; partial replays of Bronze. Catches **runtime** failures invisible to lint or unit tests. |
| **SQL validation tests** | Silent join misalignment; a Silver job whose `MERGE` keys flipped; SCD2 boundaries that opened spurious versions; Gold reading from stale Silver. These mimic the post-mortem queries you'd run after a "why is yesterday's GMV different from today's?" alert. |

Each layer is independent. CI never reaches the data; quality gates never inspect the code. Both must pass, and the SQL queries are the post-deploy sanity that keeps Gold honest.

---

## What gets produced

Final tables under `main.ecom_gold`:

| Table | Grain | Powers |
|---|---|---|
| `fact_daily_sales`     | day × category × country | Sales / GMV / AOV dashboards |
| `fact_funnel`          | day                      | View → cart → purchase rates |
| `fact_abandoned_carts` | session                  | Email & ad remarketing campaigns |
| `dim_user_360`         | user                     | RFM segmentation; ML feature base |

Each is denormalised, partitioned where it makes sense, and Z-ordered on the columns the BI layer actually filters on.

---

## Design decisions worth highlighting

1. **Decoupled producer.** Auto Loader does not know the data is synthetic — swapping for Kafka changes ~50 lines.
2. **Append-only Bronze.** Silver and Gold are always rebuildable. Bronze is the replay log.
3. **`MERGE` for facts, `overwrite` for marts.** Silver `events` is large enough that incremental MERGE pays off; Gold marts are cheap to rebuild and benefit from clean schema overwrites.
4. **SCD2 by attribute hash.** Robust to noisy `updated_ts` — only real changes open a new version.
5. **`availableNow` over continuous.** Right-sized for Free Edition; one flag to flip in production.
6. **Centralised IO helpers.** `TableRef.fqn`, `volume_path()`, `upsert/overwrite/optimize` — renaming a schema is a one-line change.

---

## Lessons from real failures

The pipeline was hardened against three failures hit during development — all documented with root cause and fix in [`docs/runbook.md`](./docs/runbook.md):

- `Catalog 'main' does not exist` — added `CREATE CATALOG IF NOT EXISTS` to `00_setup.py`.
- `ModuleNotFoundError: No module named 'faker'` — added `%pip install faker` + `dbutils.library.restartPython()` to `10_run_simulator.py`.
- `[DELTA_ZORDERING_ON_PARTITION_COLUMN] sale_date` — replaced ZORDER columns with non-partition predicates (`category`, `country`).

---

## Scaling to production

The architectural shape — medallion, event-first, decoupled producer — stays identical. Only the substrate changes.

| Concern         | This project (Free Edition)   | Production                                          |
|-----------------|-------------------------------|-----------------------------------------------------|
| Ingestion       | Auto Loader on a Volume       | Kafka / Kinesis / Event Hubs structured streaming  |
| Compute         | Single shared cluster         | Per-layer job clusters, Photon, autoscaling, spot   |
| Orchestration   | Databricks Workflows          | Workflows + Delta Live Tables                       |
| Governance      | Single UC                     | Full UC — lineage, ABAC, column masks, tagging      |
| Quality         | `Expectation` helper          | DLT expectations or Great Expectations in CI        |
| Schema          | `_rescued_data` column        | Confluent / Apicurio Schema Registry                |
| Performance     | Manual `OPTIMIZE` + Z-ORDER   | Liquid Clustering, Predictive Optimization          |
| CI/CD           | GitHub Actions (lint + tests) | + Databricks Asset Bundles ([`databricks.yml`](./databricks.yml)) |
| PII             | None (synthetic)              | Tokenisation + UC column masks                      |
| ML              | Notebook training             | Feature Store + MLflow + Model Serving              |

---

## DLT Pipeline (Alternative Execution Path)

The same Bronze → Silver → Gold logic is also packaged as a **Delta Live Tables** pipeline under [`pipelines/dlt/`](./pipelines/dlt/) (`bronze.py`, `silver.py`, `gold.py`). The DLT pipeline is **additive** — the existing notebook-based Workflow is unchanged and remains the canonical path.

```
┌─────────────────────────┐        ┌─────────────────────────┐
│ Workflow `medallion`    │        │ DLT pipeline `ecom-dlt` │
│  notebooks/00 … 99      │        │  pipelines/dlt/*.py     │
│  imperative PySpark     │        │  declarative @dlt.table │
│  Auto Loader + MERGE    │        │  Auto Loader + DLT-     │
│  manual checkpoints     │        │  managed checkpoints    │
└────────────┬────────────┘        └────────────┬────────────┘
             │                                  │
             ▼                                  ▼
   `${var.catalog}.ecom_{bronze,silver,gold}`   `${var.catalog}.${var.dlt_schema}`
```

### Job vs. DLT — when to use which

| Concern             | Workflow job                            | DLT pipeline                                       |
|---------------------|-----------------------------------------|----------------------------------------------------|
| Compute model       | Notebook tasks on a shared cluster      | DLT-managed pipeline cluster                       |
| Checkpoints         | Hand-managed under `_checkpoints/`      | Owned by the DLT runtime                           |
| Idempotency         | Delta `MERGE` + `overwrite` in `src/`   | Built-in: full materialised-view rebuild           |
| Quality gates       | `99_quality_checks` (`Expectation`)     | `@dlt.expect` / `@dlt.expect_or_fail` per table   |
| Lineage             | Implicit (notebook order)               | Explicit DAG in the DLT UI                         |
| Observability       | Workflow run page + cell output         | DLT event log + per-expectation drop counters     |
| Schema evolution    | `cloudFiles.schemaEvolutionMode=rescue` | Same, plus DLT-tracked schema versions             |
| Cost on Free Edition| Lower — reuses the shared cluster       | Higher — dedicated DLT compute                     |
| Best for…           | Small / cost-sensitive / Free Edition   | Production scale, strict SLAs, declarative ops     |

**Tradeoffs in one sentence:** the Workflow job is cheaper and simpler on Free Edition; DLT is more declarative and observable but spins its own cluster. Use the job for daily operation here, and use DLT to demonstrate (or migrate to) a fully-managed pipeline runtime.

The DLT path writes to a separate schema (`${var.dlt_schema}`, default `ecom_dlt`) so it never contends on the same Delta tables as the job.

### Deploying

```bash
# Full bundle: job + DLT pipeline
databricks bundle deploy --target dev

# DLT pipeline only — leaves the Workflow job untouched in the workspace
databricks bundle deploy --target dev_dlt_only

# Run each path
databricks bundle run --target dev medallion             # Workflow job
databricks bundle run --target dev ecom_dlt_pipeline     # DLT pipeline
```

> **Compute model.** This project runs on **Databricks Free Edition** with **managed (serverless-style) compute**. `databricks.yml` does **not** declare any `job_clusters`, `new_cluster`, `node_type_id`, or worker configuration — the workspace provisions compute implicitly for both the Workflow job and the DLT pipeline. CD runs **both** paths back-to-back (`bundle run … medallion` then `bundle run … ecom_dlt_pipeline`) and a post-run step queries `fact_daily_sales` in each Gold schema to confirm rows landed; if either pipeline fails, the workflow fails.

See [`docs/ci_cd.md`](./docs/ci_cd.md) for target / partial-deploy details and [`docs/runbook.md`](./docs/runbook.md) for first-run + debugging.

---

## Author

**Bruno Peixoto** — building production-grade data platforms on Databricks and Delta Lake.
For the full step-by-step technical breakdown of every layer, start at [`docs/README.md`](./docs/README.md).
