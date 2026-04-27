# E-commerce Data Platform — Technical Documentation

End-to-end Databricks pipeline that simulates an e-commerce site (electronics, appliances, furniture), lands clickstream and snapshot data into a Unity Catalog Volume, and processes it through a **Bronze → Silver → Gold** medallion on Delta Lake. Built and tuned for **Databricks Free Edition**, with PySpark and Delta Lake. The architecture is deliberately substrate-agnostic: replacing the synthetic producer with Kafka or Kinesis only changes the ingestion connector, not the downstream logic.

---

## 1. Project overview

The platform models the full path of an e-commerce visitor — from anonymous landing through page browsing, cart activity, and purchase — and turns the resulting events into analytics-ready marts (daily sales, conversion funnel, abandoned carts, user 360).

There are three runnable surfaces:

| Surface | Purpose |
|---|---|
| `src/simulator/` | Pure-Python event producer that writes JSON files to a Volume. |
| `src/{bronze,silver,gold}/` | PySpark transformation modules; one concern per file. |
| `notebooks/` | Thin Databricks notebooks that wire `src/` modules to widgets and run them in order. |

Outputs (in `main.ecom_gold`):

- `fact_daily_sales` — GMV / units / AOV by `sale_date × category × country`.
- `fact_funnel` — view → cart → purchase rates per day at session grain.
- `fact_abandoned_carts` — sessions with carts but no purchase, flagged as remarketable when `user_id` is known.
- `dim_user_360` — per-user RFM-style summary plus a lifecycle segment (`prospect`, `active`, `lapsing`, `churned`).

---

## 2. Architecture (medallion)

```
                    ┌──────────────────────────┐
                    │      Simulator           │
                    │  (Python, faker, RNG)    │
                    └────────────┬─────────────┘
                                 │ JSON.gz files
                                 ▼
   /Volumes/main/ecom_bronze/landing/{events,users,products}/dt=YYYY-MM-DD
                                 │
                                 │  Auto Loader (cloudFiles)
                                 ▼
   ┌──────────────── BRONZE ────────────────┐
   │ events_raw   (append, _ingest_date)    │
   │ users_raw    (append snapshots)        │
   │ products_raw (append snapshots)        │
   └────────────────────┬───────────────────┘
                        │
                        ▼
   ┌──────────────── SILVER ────────────────┐
   │ events            (dedup + sessionize) │
   │ dim_users_scd2    (SCD2)               │
   │ dim_products_scd2 (SCD2)               │
   │ fact_orders       (purchase rollup)    │
   └────────────────────┬───────────────────┘
                        │
                        ▼
   ┌──────────────── GOLD ──────────────────┐
   │ fact_daily_sales      fact_funnel      │
   │ fact_abandoned_carts  dim_user_360     │
   └────────────────────────────────────────┘
```

**Layer contracts:**

- **Bronze** — append-only, source-faithful. No business rules. Schema is **pinned** (no `inferSchema`); unexpected fields land in `_rescued_data` rather than failing the pipeline.
- **Silver** — cleansed, deduplicated, conformed. Idempotent: re-running over the same Bronze window produces the same Silver via Delta `MERGE` keyed on `event_id`.
- **Gold** — denormalised marts optimised for query speed; rebuilt from Silver via overwrite.

---

## 3. Data flow

### 3.1 Producer → Volume

`src/simulator/emit.py` writes gzipped JSON files (default 5 000 rows each) into the landing Volume. Files are partitioned by ingest date (`dt=YYYY-MM-DD`) so Auto Loader scans stay bounded as the Volume grows.

```
landing/events/dt=2026-04-27/events-<uuid>.json.gz
landing/users/users-<uuid>.json
landing/products/products-<uuid>.json
```

### 3.2 Volume → Bronze (Auto Loader)

`src/bronze/ingest_events.py` reads each landing folder with `cloudFiles` (`format=json`, `schemaEvolutionMode=rescue`, schema pinned from `src.common.schemas`). Each row is enriched with `_ingest_ts`, `_source_file`, and `_ingest_date`, then written to Delta with `partitionBy("_ingest_date")`. Default trigger is `availableNow` — process new files and exit — chosen to keep Free Edition cluster cost in check.

### 3.3 Bronze → Silver

- `silver.events` (`src/silver/events.py`) — filters invalid `event_type`, dedupes by `event_id` (latest `_ingest_ts` wins), then **sessionises** by user-time gap (`> inactivity_minutes` opens a new session). The producer's `session_id` is preserved as `session_id_raw`; the canonical key is `session_id_silver = <user_id|anon>_<seq>`.
- `silver.dim_users_scd2` and `silver.dim_products_scd2` — collapse runs of unchanged attribute hashes into one version per change, emitting `valid_from / valid_to / is_current / version` columns. Tracked columns are explicit (`updated_ts` is excluded — it changes on every snapshot even when the data is identical).
- `silver.fact_orders` — collapses multiple `purchase` line-item events sharing the same `order_id` into one row with an `items` struct array, then computes `tax`, `shipping` (free above $75, flat $9.99 otherwise), and `total`.

### 3.4 Silver → Gold

Each Gold mart reads only the Silver tables it needs and writes via `overwrite` (idempotent, `overwriteSchema=true`). All logic is in `src/gold/` and called from `notebooks/40_gold.py`.

---

## 4. Folder structure

```
databricks_ecommerce/
├── conf/
│   ├── pipeline.yml         # catalog/schema names, trigger, watermark, zorder targets
│   └── simulator.yml        # population sizes, traffic profile, behavior probabilities
├── notebooks/
│   ├── 00_setup.py          # creates catalog, schemas, landing volume + subdirs
│   ├── 10_run_simulator.py  # generates users/products/events into the volume
│   ├── 20_bronze.py         # Auto Loader streams (events, users, products)
│   ├── 30_silver.py         # silver.events, dim_*, fact_orders + OPTIMIZE
│   ├── 40_gold.py           # daily_sales, funnel, abandoned_carts, user_360 + OPTIMIZE
│   └── 99_quality_checks.py # Expectations on Silver+Gold; aborts the workflow on fail
├── src/
│   ├── common/
│   │   ├── config.py        # YAML loader with ECOM_<FILE>_<KEY> env overrides
│   │   ├── io.py            # TableRef, upsert (MERGE), overwrite, optimize, volume_path
│   │   ├── quality.py       # Expectation dataclass, evaluate(), enforce()
│   │   └── schemas.py       # Pinned StructTypes for events/users/products/orders
│   ├── simulator/
│   │   ├── entities.py      # EntityGenerator (users, products) — deterministic via seed
│   │   ├── behavior.py      # Session state machine: BROWSE→CART→CHECKOUT→PURCHASE/ABANDON
│   │   └── emit.py          # JSON(.gz) writer; partitions by dt=YYYY-MM-DD
│   ├── bronze/
│   │   └── ingest_events.py # stream_events(), stream_snapshot() — Auto Loader
│   ├── silver/
│   │   ├── events.py        # dedup + sessionize; MERGE on event_id
│   │   ├── dim_users.py     # SCD2 on email/country/city/marketing_opt_in/loyalty_tier
│   │   ├── dim_products.py  # SCD2 on name/category/subcategory/brand/price/currency/active
│   │   └── fact_orders.py   # collapse purchase events by order_id; compute tax/shipping
│   └── gold/
│       ├── daily_sales.py   # GMV/units/AOV by day × category × country
│       ├── funnel.py        # session-grain view→cart→purchase rates
│       ├── abandoned_carts.py
│       └── user_360.py      # joins dim_users_scd2 (current) + fact_orders + events
├── tests/                   # pytest unit tests for simulator and quality helpers
├── requirements.txt
├── README.md
└── documentation.md         # this file
```

---

## 5. Layer-by-layer explanation

### 5.1 Simulator

Generates a deterministic, configurable e-commerce workload.

- **Entities** (`src/simulator/entities.py`): `EntityGenerator(seed)` produces users and a product catalog. Faker is seeded so the same seed yields the same population. Categories are weighted (`electronics 0.50 / appliances 0.30 / furniture 0.20`) and each category has its own price range.
- **Behavior** (`src/simulator/behavior.py`): a session state machine
  `LANDING → BROWSE → (CART → (CHECKOUT → PURCHASE | ABANDON_CART) | EXIT)`.
  Probabilities come from `conf/simulator.yml` (`add_to_cart_probability`, `checkout_given_cart`, `purchase_given_checkout`, page-views Poisson `lambda`). Device weights, payment methods, and abandonment timeout are tunable.
- **Emission** (`src/simulator/emit.py`): batches events into JSON files (`rows_per_file=5000`, `compress=true`), writes one snapshot per `users`/`products` bootstrap.
- **Modes** (notebook widget): `bootstrap` (write users + products + events on first run), `burst` (one-shot N sessions), `continuous` (loop for `loop_minutes`).

### 5.2 Bronze (Auto Loader)

`stream_events` and `stream_snapshot` in `src/bronze/ingest_events.py` use `spark.readStream.format("cloudFiles")` with:

- Format: `json`
- Schema: pinned via `.schema(EVENT_SCHEMA)` (no inference)
- `cloudFiles.schemaEvolutionMode = "rescue"` — bad/extra fields go to `_rescued_data`
- `cloudFiles.schemaLocation = <checkpoint>/_schema`
- Output: Delta, append, `partitionBy("_ingest_date")` for events
- Trigger: `availableNow` by default (cost-aware), `continuous` (30 s) optional

The checkpoint folder (`.../_checkpoints/<source>`) is what makes re-runs idempotent — Auto Loader will not re-ingest files it has already processed.

### 5.3 Silver

Four jobs orchestrated by `notebooks/30_silver.py`:

1. **`silver.events`** — `src/silver/events.py`
   - Filters: `event_id NOT NULL`, `event_type IN VALID_EVENT_TYPES`, `event_ts NOT NULL`.
   - Dedup: `row_number()` over `(event_id ORDER BY _ingest_ts DESC)`, keep rank 1.
   - Sessionise: `lag(event_ts)` over `(user_id ORDER BY event_ts)`; new session when gap > `inactivity_minutes` (default 30). Cumulative `sum` produces `_session_seq`; `session_id_silver = <user_id|anon>_<seq>`.
   - Write: `MERGE` on `event_id` (idempotent via `src.common.io.upsert`).

2. **`silver.dim_users_scd2`** — `src/silver/dim_users.py`
   - Hash tracked attributes (`email, country, city, marketing_opt_in, loyalty_tier`) with `sha2(... 256)`.
   - Keep only rows where the hash changed compared to the previous `updated_ts` for that user — these are SCD2 boundaries.
   - Compute `valid_from = updated_ts`, `valid_to = lead(updated_ts)`, `is_current = valid_to IS NULL`, `version = row_number()`.

3. **`silver.dim_products_scd2`** — same pattern as users, tracking `name, category, subcategory, brand, price, currency, active`.

4. **`silver.fact_orders`** — `src/silver/fact_orders.py`
   - Filter `event_type = 'purchase' AND order_id IS NOT NULL`.
   - Group by `(order_id, user_id, session_id_silver, country, payment_method)`, `collect_list` line-item structs into `items`, sum `subtotal` and `total_units`.
   - Apply business rules: `tax = subtotal * 0.08`, `shipping = 0 if subtotal > 75 else 9.99`, `total = subtotal + tax + shipping`.
   - Write `overwrite` partitioned by `order_date`.

After writes, `notebooks/30_silver.py` runs `OPTIMIZE ... ZORDER BY` on hot tables: `events` on `(event_ts, user_id, session_id_silver)`, `fact_orders` on `(order_ts, user_id)`.

### 5.4 Gold

- **`fact_daily_sales`** (`src/gold/daily_sales.py`) — explodes `fact_orders.items`, groups by `(sale_date, category, country)`, computes `orders` (distinct order count), `units` (sum quantity), `gmv` (sum line_total), `avg_order_value` (avg of order total). Partitioned by `sale_date`.
- **`fact_funnel`** (`src/gold/funnel.py`) — at session grain, `max(event_type=='X')` flags for `viewed/carted/purchased/abandoned`; aggregates per day with `cart_rate`, `purchase_rate`, `abandon_rate`.
- **`fact_abandoned_carts`** (`src/gold/abandoned_carts.py`) — sums `add_to_cart` value per session, `LEFT ANTI JOIN` against sessions with a `purchase`. Sessions whose `user_id` is known are flagged `remarketable = true`.
- **`dim_user_360`** (`src/gold/user_360.py`) — joins current SCD2 `dim_users` with order metrics (count, LTV, AOV, first/last order) and event activity (sessions, last seen). `recency_days` derived from `last_order_ts`; lifecycle `segment` ∈ `{prospect, active (≤30d), lapsing (≤180d), churned}`.

After writes, `OPTIMIZE ... ZORDER BY (category, country)` on `fact_daily_sales` (the partition column `sale_date` is intentionally excluded from ZORDER — see §8).

---

## 6. Technologies used

- **Databricks (Free Edition)** — workspace, cluster, Unity Catalog, Volumes, Workflows. Free Edition implies single shared cluster, limited libraries (faker is **not** preinstalled), and one-shot triggers preferred over 24/7 streams.
- **Delta Lake** — storage format for every table; provides ACID, time travel, `MERGE`, `OPTIMIZE / ZORDER BY`, schema evolution. Bronze/Silver/Gold are all Delta managed tables under Unity Catalog.
- **PySpark** — DataFrame API for transformations (`Window` for dedup/sessionise/SCD2, `groupBy/agg` for marts, `explode` for line items). Delta Python bindings (`delta.tables.DeltaTable`) drive `MERGE`.
- **Auto Loader (`cloudFiles`)** — incremental file discovery, schema location, `schemaEvolutionMode=rescue`, durable checkpointing. Triggered with `availableNow` to behave like a batch job on Free Edition.
- **Faker** — synthetic PII (emails, names, cities) for the simulated user population. Installed at notebook start because it is not bundled with the Free Edition runtime.
- **PyYAML** — `conf/*.yml` loaded by `src.common.config.load_config` with `ECOM_<FILE>_<DOTTED_KEY_UPPER>` environment overrides.
- **pytest + chispa** — unit tests for simulator behaviour and quality helpers in `tests/`.

---

## 7. Key design decisions

1. **Decoupled producer.** The simulator writes plain JSON files into a Volume; Auto Loader doesn't know the data is synthetic. Swapping it for Kafka/Kinesis later only changes `src/bronze/ingest_events.py`.
2. **Pinned schemas, no `inferSchema`.** All schemas live in `src/common/schemas.py` with a `SCHEMA_VERSION` constant. A single bad row from upstream can silently change a column type otherwise; pinning makes drift loud.
3. **`schemaEvolutionMode = rescue`.** Unknown fields land in `_rescued_data` instead of breaking the stream. The pipeline keeps moving; data engineers see the rescued payload and decide.
4. **Append-only Bronze.** Bronze is a replay log; every Silver/Gold table can be rebuilt from it. No business rules, no dedup, no joins — those concerns belong downstream.
5. **Idempotent Silver via `MERGE` on `event_id`.** Re-running `30_silver` over an overlapping Bronze window does not produce duplicates (`src.common.io.upsert`).
6. **Re-derive `session_id_silver` from event-time gaps.** The producer's `session_id` may not survive distributed shuffles in a real Kafka topology (different shards, late events). The producer's value is preserved as `session_id_raw` for debugging.
7. **SCD2 by attribute hash, not raw equality.** `updated_ts` changes on every snapshot even when the data is identical, so we hash only the *tracked* columns. `dim_users` tracks 5 columns; `dim_products` tracks 7.
8. **`availableNow` over continuous streaming.** On Free Edition, keeping a stream alive 24/7 wastes the shared cluster. `availableNow` processes any new files and exits, ideal for Workflow scheduling.
9. **Centralised IO helpers** (`src/common/io.py`). Layers never build a path or FQN by hand: `TableRef.fqn`, `volume_path()`, `upsert/overwrite/optimize`. Renaming a schema is a one-line change.
10. **Config + env overrides** (`src/common/config.py`). The same code runs in dev and prod by setting `ECOM_PIPELINE_CATALOG=dev_main` etc. — no per-environment forks.
11. **Notebooks as thin shells.** Every notebook is widgets + imports + 1–2 line module calls. All real logic is unit-testable Python in `src/`.
12. **`overwrite` for marts, `MERGE` for facts.** Gold is cheap to rebuild and benefits from clean schema overwrites; Silver `events` is large enough that incremental MERGE pays off.

---

## 8. Common errors encountered and fixes

| # | Error | Where | Root cause | Fix |
|---|-------|-------|-----------|-----|
| 1 | `Catalog 'main' does not exist` on first run | `notebooks/00_setup.py` | Free Edition workspaces may not have the `main` catalog provisioned; the original setup assumed it existed. | Added `CREATE CATALOG IF NOT EXISTS \`{CATALOG}\`` before any schema/volume creation in `00_setup.py`. |
| 2 | `ModuleNotFoundError: No module named 'faker'` | `notebooks/10_run_simulator.py` (and `src/simulator/entities.py` import) | Faker is in `requirements.txt` for local dev but is not pre-installed on the Free Edition cluster. | Added `%pip install --quiet "faker>=24.0"` followed by `dbutils.library.restartPython()` at the top of `10_run_simulator.py`. The restart is required for the new package to be visible to subsequent cells. |
| 3 | `[DELTA_ZORDERING_ON_PARTITION_COLUMN] sale_date is a partition column. Z-Ordering can only be performed on data columns. SQLSTATE: 42P10` | `notebooks/40_gold.py` (the actual `optimize()` call lives here, not in `src/gold/daily_sales.py`) | `fact_daily_sales` is partitioned by `sale_date`; Delta forbids ZORDER on a partition column because partitioning already provides the same data-skipping benefit. | Changed `zorder=["sale_date", "category"]` to `zorder=["category", "country"]` in `notebooks/40_gold.py`. Z-ORDER now applies to two non-partition predicate columns. |
| 4 | `_rescued_data` non-empty on Bronze | `bronze.events_raw` | Producer emitted a field not in `EVENT_SCHEMA` (or a type mismatch). | Inspect rescued payloads, decide whether to extend `EVENT_SCHEMA` and bump `SCHEMA_VERSION`, or quarantine. By design the pipeline keeps running. |
| 5 | Auto Loader re-ingests everything after a checkpoint deletion | `bronze.events_raw` (any) | Checkpoint state lives at `<checkpoint_path>/_schema` and `<checkpoint_path>/offsets`. Deleting the checkpoint resets Auto Loader's bookmark. | Treat checkpoints as durable infrastructure. To force a clean reload, drop the target table *and* the checkpoint together. |

---

## 9. How to run the project — practical guide

### 9.1 Prerequisites

- A Databricks workspace with a Unity Catalog metastore (Free Edition works).
- Personal access to **Repos / Git folders**.
- For local development: Python ≥ 3.10, `pip install -r requirements.txt`.

### 9.2 First-time setup on Databricks

1. **Import the repo.** Workspace → Repos → Add repo → paste the GitHub URL.
2. **Run `notebooks/00_setup.py` once.**
   Widgets default to `catalog=main`, `bronze_schema=ecom_bronze`, `silver_schema=ecom_silver`, `gold_schema=ecom_gold`, `volume_name=landing`. Override per environment if needed. The notebook:
   - creates the catalog if missing,
   - creates the three schemas,
   - creates the `landing` Volume under `ecom_bronze`,
   - pre-creates `events/`, `users/`, `products/`, `orders/`, `_checkpoints/` subfolders,
   - prints `SHOW SCHEMAS` and the volume listing for verification.

### 9.3 Generate data

3. **Run `notebooks/10_run_simulator.py`.**
   The first two cells install `faker>=24.0` and `restartPython()`. Then choose:
   - `mode = bootstrap` → emits users + products snapshots **and** an initial event burst.
   - `mode = burst` → emits `n_sessions` events in a single shot (default 5 000).
   - `mode = continuous` → keeps emitting batches every `rotation_seconds` for `loop_minutes`.
   Files land in `/Volumes/main/ecom_bronze/landing/{events,users,products}/`.

### 9.4 Run the medallion

4. **Run `notebooks/20_bronze.py`** — Auto Loader streams the three folders into `events_raw`, `users_raw`, `products_raw`. Default trigger `availableNow` processes everything new and exits.
5. **Run `notebooks/30_silver.py`** — builds `events`, `dim_users_scd2`, `dim_products_scd2`, `fact_orders`, then `OPTIMIZE ... ZORDER BY` on hot tables.
6. **Run `notebooks/40_gold.py`** — builds the four marts (`fact_daily_sales`, `fact_funnel`, `fact_abandoned_carts`, `dim_user_360`), runs `OPTIMIZE` on `fact_daily_sales`, and displays a top-50 smoke query.
7. **Run `notebooks/99_quality_checks.py`** — evaluates `Expectation`s on Silver `events` and `fact_orders`. `enforce()` raises if any `severity="fail"` expectation has any failing row, which aborts a Workflow run.

### 9.5 Continuous operation (Workflows)

Wire the seven notebooks into a Databricks Workflow with this DAG:

```
00_setup → 10_run_simulator → 20_bronze → 30_silver → 40_gold → 99_quality_checks
```

`00_setup` is idempotent and can be left in the DAG, or run once and removed. Schedule the Workflow on a cron; with `availableNow` triggers each run is a finite job and the cluster can auto-terminate.

### 9.6 Local development & tests

```bash
pip install -r requirements.txt
pytest tests/
```

`tests/test_behavior.py`, `test_entities.py`, `test_quality.py`, and `test_config.py` cover the simulator and the YAML loader; they do not require a Databricks cluster.

### 9.7 Tuning levers

| Want to… | Edit |
|---|---|
| Change traffic profile (peak QPS, weekend multiplier) | `conf/simulator.yml → traffic` |
| Change funnel probabilities | `conf/simulator.yml → behavior` |
| Change session-gap definition | `conf/pipeline.yml → session.inactivity_minutes` (or the `30_silver` widget) |
| Move pipeline to dev catalog | `ECOM_PIPELINE_CATALOG=dev_main` env var, or override the `catalog` widget |
| Tighten Z-ORDER | `conf/pipeline.yml → optimize.zorder` (note: never include partition columns) |
| Add a new tracked SCD2 attribute | append to `TRACKED_COLS` in `src/silver/dim_users.py` or `dim_products.py` |
