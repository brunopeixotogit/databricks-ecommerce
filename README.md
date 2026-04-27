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

The full step-by-step write-up lives in [`documentation.md`](./documentation.md).

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

The pipeline was hardened against three failures hit during development — all documented with root cause and fix in [`documentation.md`](./documentation.md):

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
| CI/CD           | Manual deploy                 | Databricks Asset Bundles + GitHub Actions           |
| PII             | None (synthetic)              | Tokenisation + UC column masks                      |
| ML              | Notebook training             | Feature Store + MLflow + Model Serving              |

---

## Author

**Bruno Peixoto** — building production-grade data platforms on Databricks and Delta Lake.
For the full step-by-step technical breakdown of every layer, see [`documentation.md`](./documentation.md).
