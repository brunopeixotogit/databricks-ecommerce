# BricksShop — Lakehouse Data Platform on Databricks

A production-style **lakehouse** that simulates an e-commerce site (electronics, appliances, furniture), captures behavioural events from **two independent producers** (a real web application and a synthetic simulator), and refines the data through a **Bronze → Silver → Gold** medallion on **Delta Lake** — ending in four analytics-ready marts: daily sales, conversion funnel, abandoned carts, and a per-user 360.

> Designed and tuned to run end-to-end on **Databricks Free Edition** with managed / serverless compute. Replacing the synthetic producer with Kafka or Kinesis only changes the ingestion connector — every downstream module stays identical.

![Stack](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD8?logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![Tests](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)

---

## Why this project

Data-engineering portfolios tend to stop at "I can read a CSV with Spark." This one shows the moving parts of a real lakehouse:

- **Decoupled producers** — a web application that emits real user events and a notebook simulator that emits synthetic test data; both write to the same landing zone with the same schema, distinguished only by a `properties.source` tag.
- **Two interchangeable processing paths** — a notebook-based Workflow job (`medallion`) and a declarative Delta Live Tables pipeline (`ecom_dlt_pipeline`). Same logic, different ergonomic profiles.
- **Mode-controlled orchestration** — a single bundle job (`ecom_orchestrator`) runs simulator-only, processing-only (default), or both end-to-end based on a `mode` parameter. Production runs never generate synthetic data.
- **Pinned schemas, idempotent transforms, attribute-hash SCD2, query-tuned Gold marts, runtime quality gates** — the textbook patterns implemented end-to-end and validated against a live workspace.

---

## Architecture overview

Two independent producers, one landing zone, two interchangeable processing paths.

```
   ┌────────────────────────┐        ┌────────────────────────┐
   │   Web app (FastAPI)    │        │   Simulator notebook    │
   │   web/  → real users   │        │   notebooks/11_simulate │
   │   properties.source    │        │   properties.source     │
   │     = "web"            │        │     = "simulator"       │
   └───────────┬────────────┘        └────────────┬────────────┘
               │                                  │
               │  NDJSON, Files API / dbutils     │
               ▼                                  ▼
   ┌──────────────────────────────────────────────────────────┐
   │  Landing volume (single source of truth)                 │
   │  /Volumes/<catalog>/ecom_bronze/landing/events/          │
   │      └─ dt=YYYY-MM-DD/events_<uuid>.ndjson               │
   └───────────────────────────┬──────────────────────────────┘
                               │  Auto Loader (cloudFiles · schemaEvolutionMode=rescue)
                               ▼
                ┌──────────────────────────────┐
                │      Bronze (append-only)    │
                │  events_raw · users_raw      │
                │  products_raw                │
                └──────────────┬───────────────┘
                               │  PySpark · Delta MERGE · Window functions
                               ▼
                ┌──────────────────────────────┐
                │   Silver (cleansed + SCD2)   │
                │  events · dim_users_scd2     │
                │  fact_orders · dim_products  │
                └──────────────┬───────────────┘
                               │  groupBy / agg / explode · OPTIMIZE ZORDER
                               ▼
                ┌──────────────────────────────┐
                │       Gold (BI marts)        │
                │  fact_daily_sales · funnel   │
                │  abandoned_carts · user_360  │
                └──────────────────────────────┘
```

**Layer contracts**

| Layer | Contract | Idempotency mechanism |
|---|---|---|
| Bronze | Source-faithful, append-only, no business rules | Auto Loader checkpoint + `_rescued_data` |
| Silver | Cleansed, deduped, conformed, sessionised, SCD2 | Delta `MERGE` on `event_id`; SCD2 by attribute hash |
| Gold  | Denormalised marts optimised for query speed     | Full `overwrite` (`overwriteSchema=true`) |

The **simulator is fully decoupled** from the processing layer. The DLT pipeline (`pipelines/dlt/`) reads from the landing volume and never invokes the simulator. The simulator is a separate notebook (`notebooks/11_simulate.py`) that you trigger explicitly.

---

## How to run

### Prerequisites

- Databricks workspace with Unity Catalog (Free Edition works).
- `databricks` CLI authenticated against the workspace (`databricks auth login`).
- `databricks bundle deploy --target dev` to deploy the bundle resources (medallion job + DLT pipeline + orchestrator job).

### Three execution modes

The bundle exposes a single orchestrator job parameterised by a `mode` variable. Default is `prod` — production runs never generate synthetic data.

```bash
# Production (DLT only) — the safe default
databricks bundle run --target dev ecom_orchestrator --var=mode=prod

# Simulator only — generates test data, no processing
databricks bundle run --target dev ecom_orchestrator --var=mode=simulator

# Full pipeline — simulator first, then DLT
databricks bundle run --target dev ecom_orchestrator --var=mode=full
```

| Mode | Simulator runs? | DLT pipeline runs? | When to use |
|---|---|---|---|
| `prod` (default) | ❌ excluded | ✅ runs | Production processing of whatever's already in the landing zone |
| `simulator` | ✅ runs | ❌ excluded | Backfill test data without triggering processing |
| `full` | ✅ runs first | ✅ runs after | End-to-end demo or integration test |

Override the event volume too: `--var=mode=full --var=n_events=10000`. Or skip the deploy and pass run-level parameters directly: `--params mode=full --params n_events=10000`.

### CI/CD entry point

Every push to `main` that touches `src/`, `notebooks/`, `pipelines/`, `databricks.yml`, `conf/`, or `.github/workflows/` triggers `.github/workflows/cd.yml`. CD does **not** run the simulator — only `ecom_dlt_pipeline` is auto-executed, and a post-run query (`SELECT * LIMIT 10` from `dev_main.ecom_dlt.fact_daily_sales`) is the success gate. Docs-only commits skip CD entirely.

For the full CI/CD design — workflow files, path filters, deploy targets, secrets — see [`docs/ci_cd.md`](./docs/ci_cd.md).

---

## Simulation

The simulator generates a session-state-machine stream of synthetic events that conform exactly to the Bronze schema, so the same `events_raw` / `events` / `fact_orders` machinery ingests it without distinction from real traffic.

```
LANDING → BROWSE → (CART → (CHECKOUT → PURCHASE | ABANDON) | EXIT)
```

Mix is realistic: 62 % mobile / 30 % desktop / 8 % tablet, Poisson-distributed page views, per-category price ranges, tunable funnel probabilities (`conf/simulator.yml`). **Deterministic for the same seed**, so backfills are reproducible.

### How to run the simulator

| From | How |
|---|---|
| **Bundle (mode=simulator)** | `databricks bundle run --target dev ecom_orchestrator --var=mode=simulator --var=n_events=5000` |
| **Notebook directly** | Open `notebooks/11_simulate.py` in the workspace, set widgets (`n_events`, `dt`), run all cells. |
| **Python (offline)** | `from src.simulator.api import generate_events; generate_events(n_events=5000, landing_root="/tmp/landing")` — used by tests. |

### Where data lands

```
/Volumes/<catalog>/ecom_bronze/landing/events/
    └─ dt=YYYY-MM-DD/
        └─ events_<run_uuid>.ndjson
```

Each call to `generate_events()` writes **one NDJSON file** containing exactly `n_events` events. `event_id` values are fresh UUID4s per row, and a `properties.run_id` value tags every event in the same file with a shared run identifier — re-running with identical parameters never produces a colliding `event_id`, so Silver's `MERGE` on `event_id` stays idempotent.

### Controlling volume

| Lever | Where | Default |
|---|---|---|
| `n_events` | bundle var / notebook widget / function arg | `5000` |
| `dt` (target partition) | bundle / widget / arg, `"YYYY-MM-DD"` | today (UTC) |
| Funnel probabilities, device mix, anonymous-session rate | `conf/simulator.yml` | tuned |
| Catalog / schema / volume | bundle vars (`catalog`, `bronze_schema`, `volume`) | `dev_main` / `ecom_bronze` / `landing` |

---

## Web application

A small full-stack app under [`web/`](./web/README.md) — FastAPI backend + vanilla-JS storefront — that emits real user events into the same landing volume.

- Click around the storefront → `tracker.js` fires `page_view` / `add_to_cart` / `purchase` / `abandon_cart` events
- Browser batches and POSTs to `/events/batch`
- Backend uploads each batch as one NDJSON file to `/Volumes/<catalog>/ecom_bronze/landing/events/dt=YYYY-MM-DD/events_<uuid>.ndjson`
- Auto Loader / DLT pick it up on the next trigger — no different from any other event source

Every web event carries `properties.source = "web"`. Combined with the simulator's `properties.source = "simulator"`, you can split producers in any analytics query:

```sql
SELECT properties['source'] AS producer,
       event_type,
       COUNT(*) AS rows
FROM   dev_main.ecom_silver.events
WHERE  event_date = current_date()
GROUP  BY 1, 2
ORDER  BY 3 DESC;
```

Run the web app locally (single `uvicorn` process serves API + frontend on `:8000`):

```bash
pip install -r web/backend/requirements.txt
cp web/config/.env.example web/config/.env       # fill in DATABRICKS_HOST + DATABRICKS_TOKEN
uvicorn web.backend.app:app --reload --env-file web/config/.env
```

Full operational guide in [`web/README.md`](./web/README.md).

---

## Data model

| Layer | Table | Grain | What lives here |
|---|---|---|---|
| Bronze | `events_raw` | one row per ingested event | append-only, partitioned by `_ingest_date`, schema-pinned |
| Bronze | `users_raw`, `products_raw` | one row per snapshot | source-faithful entity captures |
| Silver | `events` | one row per `event_id` | deduped, sessionised (`session_id_silver`), partitioned by `event_date` |
| Silver | `dim_users_scd2`, `dim_products_scd2` | one row per attribute change | SCD2 with `valid_from / valid_to / is_current / version`; boundaries detected by hashing tracked columns only (`updated_ts` deliberately excluded so noisy snapshots don't open spurious versions) |
| Silver | `fact_orders` | one row per `order_id` | purchase-event rollup; `items` as `ARRAY<STRUCT<…>>`; `subtotal / tax (8%) / shipping (free over $75) / total` |
| Gold | `fact_daily_sales` | day × category × country | GMV / orders / units / AOV |
| Gold | `fact_funnel` | day | view / cart / purchase / abandon rates |
| Gold | `fact_abandoned_carts` | session | remarketing candidates with cart value |
| Gold | `dim_user_360` | user (current SCD2 only) | recency / orders / LTV / RFM segment |

Schemas are pinned in `src/common/schemas.py` (Workflow path) and a lockstep mirror at `pipelines/dlt/schemas.py` (DLT path — the runtime cannot import `src/`). Bumping `SCHEMA_VERSION` is a coordinated breaking change documented in [`docs/data_model.md`](./docs/data_model.md).

---

## Repository layout

```
bricksshop/
├── conf/                       # pipeline + simulator config (env-overridable)
├── notebooks/
│   ├── 00_setup.py             # catalogs, schemas, landing volume
│   ├── 01_create_tables.py     # explicit Bronze DDL
│   ├── 10_run_simulator.py     # legacy simulator (used by `medallion` job)
│   ├── 11_simulate.py          # NEW — decoupled simulator (used by `ecom_orchestrator`)
│   ├── 20_bronze.py            # Auto Loader streams (Workflow path)
│   ├── 30_silver.py            # dedup + sessionise + SCD2 + fact_orders
│   ├── 40_gold.py              # 4 marts + OPTIMIZE / ZORDER
│   └── 99_quality_checks.py    # quality gate
├── pipelines/dlt/              # DLT pipeline (declarative, serverless)
│   ├── schemas.py              # lockstep mirror of src/common/schemas.py
│   ├── bronze.py               # streaming tables via Auto Loader
│   ├── silver.py               # materialised views + @dlt.expect[_or_fail]
│   └── gold.py                 # 4 marts as materialised views
├── src/
│   ├── common/                 # TableRef, IO helpers, schemas, config loader, quality
│   ├── simulator/
│   │   ├── api.py              # NEW — generate_events(n_events, date) public API
│   │   ├── behavior.py         # session FSM
│   │   ├── entities.py         # users + products generators
│   │   ├── emit.py             # legacy file writer (gzipped JSON)
│   │   └── run.py              # legacy orchestration (used by 10_run_simulator)
│   ├── bronze/                 # Auto Loader streams (Workflow path)
│   ├── silver/                 # dedup + sessionise, SCD2, fact_orders
│   └── gold/                   # daily_sales, funnel, abandoned_carts, user_360
├── tests/                      # pytest + chispa unit tests
├── web/                        # BricksShop storefront + FastAPI backend
│   ├── backend/                # /event /events/batch /simulate /health
│   ├── frontend/               # HTML + Bootstrap + vanilla JS
│   ├── config/                 # .env template
│   └── README.md
├── docs/                       # English documentation
├── databricks.yml              # Asset Bundle (variables, resources, targets)
└── README.md                   # you are here
```

---

## Documentation index

Concise documentation in [`docs/`](./docs/README.md):

| Doc | Read this for |
|---|---|
| [`docs/architecture.md`](./docs/architecture.md)     | End-to-end design, data flow, principles, tradeoffs |
| [`docs/dlt_pipeline.md`](./docs/dlt_pipeline.md)     | DLT pipeline: code structure, execution model, deployment |
| [`docs/bronze_layer.md`](./docs/bronze_layer.md)     | Auto Loader, schema enforcement, append-only contract |
| [`docs/silver_layer.md`](./docs/silver_layer.md)     | Dedup, sessionisation, SCD2 by attribute hash |
| [`docs/gold_layer.md`](./docs/gold_layer.md)         | Marts, KPIs, BI readiness |
| [`docs/data_model.md`](./docs/data_model.md)         | Pinned schemas, ER relationships |
| [`docs/data_quality.md`](./docs/data_quality.md)     | Expectation framework, severity, failure handling |
| [`docs/ci_cd.md`](./docs/ci_cd.md)                   | GitHub Actions, deploy targets, mode wiring |
| [`docs/runbook.md`](./docs/runbook.md)               | Step-by-step run, debugging, reprocessing |
| [`web/README.md`](./web/README.md)                   | Web app: API reference, schema parity, end-to-end test plan |

---

## Scaling to production

The architectural shape — medallion, event-first, decoupled producers — stays identical. Only the substrate changes.

| Concern | This project (Free Edition) | Production |
|---|---|---|
| Producer | Web app + notebook simulator | Web app + Kafka / Kinesis / Event Hubs |
| Ingestion | Auto Loader on a Volume | Same Auto Loader against the streaming source |
| Compute | Shared cluster + serverless DLT | Per-layer job clusters · Photon · spot · serverless DLT |
| Orchestration | Asset Bundles + `mode` parameter | Same; tag-gated prod CD pipeline |
| Performance | Manual `OPTIMIZE` + Z-ORDER | Liquid Clustering · Predictive Optimization |
| Quality | `Expectation` helper + `@dlt.expect_or_fail` | Same + Great Expectations in CI |
| Schema | `_rescued_data` column | Confluent / Apicurio Schema Registry |

---

## Author

**Bruno Peixoto** — building production-grade data platforms on Databricks and Delta Lake. For the full step-by-step technical breakdown of every layer, start at [`docs/README.md`](./docs/README.md).

---

## 🇧🇷 Português (BR)

**BricksShop** é uma plataforma estilo lakehouse que simula um e-commerce, captura eventos de comportamento de **dois produtores independentes** (uma aplicação web real e um simulador sintético em notebook), e refina os dados em uma medallion **Bronze → Silver → Gold** sobre **Delta Lake** — terminando em quatro marts prontos para análise: vendas diárias, funil de conversão, carrinhos abandonados e user 360.

### Arquitetura em uma frase

```
Web app  ─┐
          ├─►  Volume de landing  ─►  Bronze  ─►  Silver  ─►  Gold
Simulador─┘
```

Os dois produtores escrevem **NDJSON** no mesmo Volume Unity Catalog (`/Volumes/<catalog>/ecom_bronze/landing/events/dt=YYYY-MM-DD/`) com o **mesmo schema**. O único diferenciador é `properties.source = "web" | "simulator"`. O DLT pipeline é totalmente independente: lê o Volume e nunca dispara o simulador.

### Modos de execução

O job `ecom_orchestrator` é controlado por uma variável `mode`:

| Modo | O que roda | Para quê |
|---|---|---|
| `prod` (default) | Apenas DLT | Produção — nunca gera dados sintéticos |
| `simulator` | Apenas simulador | Backfill de dados de teste sem disparar processamento |
| `full` | Simulador + DLT | Demo end-to-end ou teste de integração |

```bash
databricks bundle run --target dev ecom_orchestrator --var=mode=prod        # default
databricks bundle run --target dev ecom_orchestrator --var=mode=simulator
databricks bundle run --target dev ecom_orchestrator --var=mode=full
```

### Onde aprender mais (PT-BR)

Há um guia de estudo passo a passo, inteiramente em PT-BR, em [`docs_ptbr/00_guia_de_estudo.md`](./docs_ptbr/00_guia_de_estudo.md) (não versionado — material de estudo local). Ele cobre o projeto na ordem real de execução dos dados: **setup → simulator → Bronze → Silver → Gold → quality → Workflow → DLT → CI/CD**, com armadilhas comuns em cada etapa.

A documentação técnica oficial está em inglês sob [`docs/`](./docs/README.md).
