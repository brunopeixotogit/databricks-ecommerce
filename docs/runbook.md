# Runbook

Operational guide for running, debugging, and reprocessing the pipeline. The architecture and logic live in the layer-specific docs; this file is the **what to actually do** companion.

---

## 1 · Run sequence

The pipeline is one Workflow with a linear DAG:

```
00_setup ─► 01_create_tables ─► 10_run_simulator ─► 20_bronze
       ─► 30_silver ─► 40_gold ─► 99_quality_checks
```

| # | Notebook | Purpose | Idempotent? |
|---|---|---|---|
| 00 | `notebooks/00_setup.py`              | Catalog + schemas + Volume + landing subdirs | yes (`CREATE … IF NOT EXISTS`) |
| 01 | `notebooks/01_create_tables.py`      | Explicit Bronze DDL (partitioning + properties) | yes |
| 10 | `notebooks/10_run_simulator.py`      | `%pip install faker` → bootstrap entities → emit events | yes (additive; UUID file names) |
| 20 | `notebooks/20_bronze.py`             | Auto Loader streams (events, users, products) | yes (checkpoint) |
| 30 | `notebooks/30_silver.py`             | Dedup / sessionise / SCD2 / fact_orders | yes (`MERGE` + `overwrite`) |
| 40 | `notebooks/40_gold.py`               | Four marts + `OPTIMIZE ZORDER` | yes (`overwrite` + `overwriteSchema`) |
| 99 | `notebooks/99_quality_checks.py`     | Expectations gate | yes |

The DAG is end-to-end idempotent: running it twice over an unchanged input produces a Gold byte-equivalent (modulo Delta history).

The full DAG was validated end-to-end against the dev workspace (run `136243658787310`, ~4m 35s wall-clock from `RUNNING` → `TERMINATED SUCCESS`); the same path now executes automatically on every push to `main` that touches pipeline-relevant paths, via [`.github/workflows/cd.yml`](../.github/workflows/cd.yml). Docs-only pushes are intentionally skipped — see § 1.1 *When CD does not run*.

---

## 1.1 · Canonical execution path — `git push`

The pipeline runs automatically. **Manual execution (§ 2 below) is only for first-time setup and debugging.**

```
git push origin main
        │
        ▼
  CI (.github/workflows/ci.yml)
        │   ruff · pytest matrix (Py 3.10/3.11/3.12) · import smoke · conf validation · gitleaks
        ▼
  CD (.github/workflows/cd.yml)
        │   databricks bundle validate --target dev
        │   databricks bundle deploy   --target dev
        │   databricks bundle run      --target dev medallion
        ▼
  Databricks Workflow runs (7 tasks)
        │   setup → create_tables → simulate → bronze → silver → gold → quality
        ▼
  Gold tables refreshed in dev_main.ecom_gold
```

A typical merge takes **~6 minutes wall-clock**: ~90 s CI matrix → ~30 s deploy → ~4 min DAG execution.

### When CD does **not** run

CD is path-filtered. A push to `main` only triggers `cd.yml` when at least one of these paths changed:

```
src/**   notebooks/**   databricks.yml   conf/**   .github/workflows/**
```

A **docs-only commit** (`README.md`, `docs/**`, any `*.md`) still runs CI but does **not** deploy to Databricks — saves workspace compute and avoids redundant job runs. To force a redeploy without a code change (e.g. after configuring secrets), use **Actions → CD — deploy to dev → Run workflow** (`workflow_dispatch` ignores the path filter).

### Watching a deploy

- **GitHub Actions** — https://github.com/brunopeixotogit/databricks-ecommerce/actions
- **Databricks job** — https://dbc-98593ae9-08e1.cloud.databricks.com/jobs/795815936497947?o=3252836371316438
- **Job logs / latest run** — open the job page above and click the most recent run row.

### Debugging a CI failure

| Failing job | First place to look | Common causes |
|---|---|---|
| `lint-and-test (3.10/3.11/3.12)` | "Run pytest with coverage" step log | New unit test failure; coverage dropped below `fail_under = 60`; ruff violations introduced |
| Import-graph smoke | "Verify src/ import structure" step | A pure-Python module added `from pyspark.sql import …` (use `if TYPE_CHECKING:` guard or move to a Spark-bound module) |
| `config-validation` | "Load every YAML in conf/" step | Malformed YAML; `load_config` schema regression |
| `secrets-scan` | Gitleaks step output | False positives are advisory (`continue-on-error: true`). Real findings need rotation + history rewrite |

To reproduce CI locally before pushing:

```bash
pip install -e ".[dev]"
ruff check src tests
pytest --cov=src --cov-report=term
```

### Debugging a CD failure

| Symptom | Cause | Remedy |
|---|---|---|
| `Error: required environment variable DATABRICKS_HOST is not set` | `dev` GitHub Environment secrets unconfigured | Settings → Environments → `dev` → add `DATABRICKS_HOST_DEV` and `DATABRICKS_TOKEN_DEV` |
| `bundle validate` fails with reference error | `databricks.yml` typo or missing `${var.*}` value | Run `databricks bundle validate --target dev` locally |
| Deploy succeeds, run fails on a specific task | Notebook-level error | Open the Databricks job run page; click the failing task; inspect cell output |
| `quality` task raises `AssertionError` | Data-quality predicate in `99_quality_checks` violated | Inspect the failing predicate — see [`data_quality.md`](./data_quality.md); fix upstream and re-trigger CD via **Re-run all jobs** in the GitHub Actions UI |

### Re-running the Databricks job manually

CD runs the medallion automatically on every push that touches pipeline-relevant paths (see § 1.1 path filter). To trigger an extra run **without any commit** (e.g. after fixing a transient issue, producer drift, or after configuring secrets):

```bash
# From your laptop — same auth as CD uses
databricks bundle run --target dev medallion
```

Or in the workspace UI: open the job page → **Run now**. Either path uses the bundle's already-deployed Job definition.

To re-trigger CD itself (for example to test a re-deploy without a commit), use **Actions → CD — deploy to dev → Run workflow**.

---

## 1.2 · DLT pipeline (alternative execution path)

The same medallion logic also runs as a **Delta Live Tables** pipeline declared at [`pipelines/dlt/`](../pipelines/dlt/). The Workflow described in §1 remains the canonical path on Free Edition; the DLT pipeline is an additive alternative for environments where DLT's declarative model and managed compute are preferred. Both paths share the landing Volume but write to **different schemas**, so they never contend on the same Delta table.

### Choosing a target

| Target          | Deploys                                      | When to use                                          |
|-----------------|----------------------------------------------|------------------------------------------------------|
| `dev` (default) | Workflow job + DLT pipeline                  | Standard dev work — both paths available             |
| `dev_dlt_only`  | DLT pipeline only                            | Iterating on `pipelines/dlt/*` without touching the job |
| `prod`          | Workflow job + DLT pipeline (production mode)| Tag-gated production promotion                       |

### Running the DLT pipeline

```bash
# Validate first
databricks bundle validate --target dev

# Deploy + run (full bundle)
databricks bundle deploy --target dev
databricks bundle run    --target dev ecom_dlt_pipeline

# Or: deploy only the DLT pipeline (job is untouched)
databricks bundle deploy --target dev_dlt_only
databricks bundle run    --target dev_dlt_only ecom_dlt_pipeline
```

In the workspace UI: **Pipelines → ecom-dlt → Start**. The DLT event log is at **Pipelines → ecom-dlt → Event log** (the per-expectation drop counts surface there too).

### What gets created

The DLT pipeline publishes every table to the schema named by `${var.dlt_schema}` (default `ecom_dlt`):

| Layer | Tables (under `${catalog}.${dlt_schema}`)                                        |
|-------|-----------------------------------------------------------------------------------|
| Bronze| `events_raw`, `users_raw`, `products_raw`                                         |
| Silver| `events`, `dim_users_scd2`, `dim_products_scd2`, `fact_orders`                    |
| Gold  | `fact_daily_sales`, `fact_funnel`, `fact_abandoned_carts`, `dim_user_360`         |

Table names match the Workflow path; only the schema differs. SQL templates pointing at `dev_main.ecom_silver.events` still work against `dev_main.ecom_dlt.events` with one find-and-replace.

### Debugging DLT failures

| Symptom | First place to look | Common causes / remedies |
|---|---|---|
| Pipeline update fails on `events_raw` | DLT event log → "Update progress" tab → click failed table | Landing path missing or empty (`/Volumes/<cat>/<bronze_schema>/landing/events`); pinned schema mismatched producer (check `_rescued_data` on bronze) |
| `expect_or_fail` aborts an update | Same UI; "Data quality" panel shows which expectation tripped | Same root-cause list as `99_quality_checks` (see §5.8). Inspect failing rows directly under `${dlt_schema}.<table>` |
| Update succeeds but tables are empty | Pipeline configuration → check `ecom.catalog`, `ecom.bronze_schema`, `ecom.volume` | Configuration block in `databricks.yml` not picked up — re-deploy with `--target dev` |
| `import dlt` fails locally / in CI | `import dlt` only resolves in the DLT runtime | Do not run `pipelines/dlt/*.py` outside DLT; the CI lane intentionally does not import these files |
| Expected schema not created | Unity Catalog permissions | The principal running the pipeline needs `CREATE SCHEMA` on the catalog; for Free Edition this is the workspace user |

### Rollback / cleanup

The DLT path is independent of the Workflow path. To remove DLT artefacts without touching the job:

```bash
databricks bundle destroy --target dev_dlt_only
```

Or drop the schema directly: `DROP SCHEMA IF EXISTS ${catalog}.${dlt_schema} CASCADE;`. The Workflow job's tables in `ecom_bronze` / `ecom_silver` / `ecom_gold` are unaffected.

---

## 2 · First-time setup on Databricks Free Edition

### 2.1 Prerequisites

- Databricks workspace with a Unity Catalog metastore (Free Edition works).
- Personal access via **Workspace → Repos → Add Repo**.
- A running cluster (Free Edition: the shared cluster).

### 2.2 Steps

1. **Import the repo.** Workspace → Repos → Add Repo → paste the GitHub URL.
2. **Run `notebooks/00_setup.py`.** Defaults: `catalog=main`, `bronze_schema=ecom_bronze`, `silver_schema=ecom_silver`, `gold_schema=ecom_gold`, `volume_name=landing`. Override per environment if needed.
   - Creates the catalog if missing (`CREATE CATALOG IF NOT EXISTS`).
   - Creates the three schemas.
   - Creates the `landing` Volume under `ecom_bronze`.
   - Pre-creates `events/`, `users/`, `products/`, `orders/`, `_checkpoints/` subfolders.
   - Prints `SHOW SCHEMAS` and the Volume listing for verification.
3. **Run `notebooks/01_create_tables.py`.** Pre-creates the three Bronze tables with explicit DDL (`delta.appendOnly=true`, `_ingest_date` partitioning on events).
4. **Run `notebooks/10_run_simulator.py`.** First two cells install `faker` and `restartPython()`. Choose a mode:
   - `bootstrap` — emits users + products snapshots **and** an initial event burst.
   - `burst` — emits `n_sessions` events (default 5 000).
   - `continuous` — keeps emitting batches every `rotation_seconds` for `loop_minutes`.
   Files land in `/Volumes/main/ecom_bronze/landing/{events,users,products}/`.
5. **Run `notebooks/20_bronze.py`.** Auto Loader streams the three folders into `events_raw`, `users_raw`, `products_raw`. Default trigger `availableNow` processes everything new and exits.
6. **Run `notebooks/30_silver.py`.** Builds `events`, `dim_users_scd2`, `dim_products_scd2`, `fact_orders`; runs `OPTIMIZE … ZORDER BY` on hot tables.
7. **Run `notebooks/40_gold.py`.** Builds the four marts; runs `OPTIMIZE` on `fact_daily_sales`; displays a top-50 smoke query.
8. **Run `notebooks/99_quality_checks.py`.** Evaluates expectations on Silver. `enforce()` raises if any `severity="fail"` predicate has any failing row, which aborts a Workflow run.

### 2.3 Continuous operation — wire into a Workflow

Workspace → Workflows → Create Job → add seven tasks with the linear DAG above. Schedule via cron (e.g. hourly off-peak). With `availableNow` triggers each run is finite and the cluster auto-terminates.

---

## 3 · Local execution (without a Databricks cluster)

The simulator and quality helpers are pure Python. Most logic can be tested without a Databricks cluster:

```bash
git clone https://github.com/brunopeixotogit/databricks-ecommerce.git
cd databricks-ecommerce

# Default lane — pure-Python, no Spark
pip install -e ".[dev]"
pytest tests/

# Optional Spark lane (~300 MB install)
pip install -e ".[dev,spark]"
pytest -m spark
```

What runs locally:
- `tests/test_entities.py` — deterministic user/product generation
- `tests/test_behavior.py` — session FSM
- `tests/test_config.py` — YAML loader + env overrides
- `tests/test_quality.py` — Expectation framework with a `FakeDF` mock
- `tests/test_run.py` — orchestration glue (determinism, contract conformance)

What doesn't run locally:
- Auto Loader, Delta MERGE, ZORDER — Databricks runtime concerns.

---

## 4 · Tuning levers

Most behavioural tuning happens through `conf/*.yml` or notebook widgets — no code edits.

| Want to… | Where | How |
|---|---|---|
| Change traffic profile (peak QPS, weekend multiplier) | `conf/simulator.yml` | Edit `traffic` section |
| Change funnel probabilities (carting / checkout / purchase) | `conf/simulator.yml` | Edit `behavior` section |
| Change session-gap definition | Widget on `30_silver` (or `conf/pipeline.yml → session.inactivity_minutes`) | Default 30 min |
| Move pipeline to dev catalog | `ECOM_PIPELINE_CATALOG=dev_main` env var, or override `catalog` widget | Same code path |
| Tighten Z-ORDER targets | `conf/pipeline.yml → optimize.zorder` | Never include partition columns |
| Add a new tracked SCD2 attribute | `src/silver/dim_users.py` or `dim_products.py` | Append to `TRACKED_COLS` |
| Increase event throughput | Widget `n_sessions` on `10_run_simulator` | Default 5 000 |
| Switch streaming to continuous | `conf/pipeline.yml → streaming.trigger`, or widget on `20_bronze` | `processingTime=30 seconds` |

---

## 5 · Debugging guide

### 5.1 Bronze stream produces zero rows

**Symptom:** `20_bronze` runs without error but `events_raw` row count stays at 0.

**Diagnosis:**
```python
# Verify the simulator actually wrote files
display(dbutils.fs.ls("/Volumes/main/ecom_bronze/landing/events"))

# Verify the Auto Loader checkpoint
display(dbutils.fs.ls("/Volumes/main/ecom_bronze/landing/_checkpoints/events/sources"))
```

**Common causes:**
- Producer wrote to a path Auto Loader isn't watching.
- Checkpoint is for a different `landing_path` than the stream's current value.

**Remedy:** confirm the `landing_path` widget matches the simulator output dir. If the checkpoint is stale, drop the target table **and** delete the checkpoint folder together — they are paired durable infrastructure.

### 5.2 `_rescued_data` is non-empty

**Symptom:** `events_raw._rescued_data IS NOT NULL` returns rows.

**Diagnosis:**
```sql
SELECT _rescued_data, _source_file
FROM   main.ecom_bronze.events_raw
WHERE  _rescued_data IS NOT NULL
LIMIT  20;
```

**Remedy:** producer drift. Either bump `SCHEMA_VERSION` and add the new field to the pinned schema, or fix the producer to emit the contract.

### 5.3 SCD2 dimension explodes to one row per snapshot

**Symptom:** `silver.dim_users_scd2` has ~one row per user *snapshot* instead of per user *change*.

**Diagnosis:** confirm `TRACKED_COLS` excludes `updated_ts`. Hashing `updated_ts` opens a spurious version on every snapshot — that's the bug.

**Remedy:** see `src/silver/dim_users.py` — `TRACKED_COLS = ["email", "country", "city", "marketing_opt_in", "loyalty_tier"]`.

### 5.4 `[DELTA_ZORDERING_ON_PARTITION_COLUMN]`

**Symptom (verbatim):**
> `[DELTA_ZORDERING_ON_PARTITION_COLUMN] sale_date is a partition column. Z-Ordering can only be performed on data columns. SQLSTATE: 42P10`

**Cause:** `OPTIMIZE … ZORDER BY (sale_date, …)` with `sale_date` declared as a partition column.

**Remedy:** remove the partition column from the ZORDER list. In `notebooks/40_gold.py`, the working version is `zorder=["category", "country"]`. Partitioning already provides skipping on `sale_date`.

### 5.5 `ModuleNotFoundError: No module named 'faker'`

**Cause:** Free Edition runtime does not pre-install `faker`.

**Remedy:** ensure `notebooks/10_run_simulator.py` starts with:
```python
# %pip install --quiet "faker>=24.0"
# dbutils.library.restartPython()
```
Both cells are required: `restartPython()` makes the freshly installed package visible to subsequent cells.

### 5.6 `Catalog 'main' does not exist`

**Cause:** Free Edition workspace doesn't have `main` provisioned.

**Remedy:** `notebooks/00_setup.py` runs `CREATE CATALOG IF NOT EXISTS \`{CATALOG}\`` before any schema/volume creation. If it's still failing, the running user lacks `CREATE CATALOG` privilege — escalate to a metastore admin.

### 5.7 Silver row count exceeds Bronze

**Cause:** dedup window is keying on a column that is null for some rows. Silver should always be ≤ Bronze.

**Remedy:** confirm `silver.events` filters `event_id IS NOT NULL` **before** the dedup window. The current code does — if you see this error, something in Bronze has changed shape.

### 5.8 Quality gate fails

**Symptom:** `99_quality_checks` raises `AssertionError: Quality gate failed: <name> (<failing>/<total>)`.

**Diagnosis:**
```python
# Re-run the evaluator without enforce
event_results = evaluate(events, event_expectations)
display(spark.createDataFrame(event_results))
```
Inspect the offending rows directly:
```sql
SELECT * FROM main.ecom_silver.events
WHERE  NOT (event_id IS NOT NULL)         -- substitute the failing predicate
LIMIT  20;
```

**Remedy:** depends on the predicate. If a producer drifted, fix upstream and reprocess. If the predicate is wrong, update it in `notebooks/99_quality_checks.py`.

---

## 6 · Reprocessing strategy

Different failure modes call for different reprocessing scopes. Pick the smallest that solves the problem.

### 6.1 Re-run a single Silver/Gold table (no Bronze change)

Just re-run the relevant notebook cell:

```
30_silver  → re-runs MERGE (events) + overwrite (dims, fact_orders)
40_gold    → re-runs overwrite for all four marts
```

Safe because Silver `MERGE` is idempotent on `event_id` and Gold is full-rebuilt.

### 6.2 Backfill from a specific Bronze window

`silver.events` is the only `MERGE` target. To replay a 7-day window:

```python
# In a temporary notebook cell, scoped re-build
window_df = spark.read.table("main.ecom_bronze.events_raw").where(
    "_ingest_date BETWEEN '2026-04-20' AND '2026-04-27'"
)
# ... apply the same Silver transformations ...
upsert(spark, target=TableRef(...), source=processed, keys=["event_id"])
```

The `MERGE` ensures rows already in Silver are updated, not duplicated. Then re-run `40_gold` and `99_quality_checks`.

### 6.3 Full replay from Bronze

Drop Silver + Gold tables, re-run `30_silver` and `40_gold`. Bronze is the durable replay log; nothing else is destroyed.

```sql
DROP TABLE IF EXISTS main.ecom_silver.events;
DROP TABLE IF EXISTS main.ecom_silver.dim_users_scd2;
DROP TABLE IF EXISTS main.ecom_silver.dim_products_scd2;
DROP TABLE IF EXISTS main.ecom_silver.fact_orders;
DROP TABLE IF EXISTS main.ecom_gold.fact_daily_sales;
-- ... etc
```

Then run the medallion notebooks in order. **Do not** delete Bronze tables or Auto Loader checkpoints — those are the durable layer.

### 6.4 Time-travel for forensic debugging

```sql
-- "What did Silver events look like as of yesterday's run?"
SELECT *
FROM   main.ecom_silver.events VERSION AS OF 412
LIMIT  20;

-- "What changed between version 412 and 413?"
DESCRIBE HISTORY main.ecom_silver.events;
```

Combined with Bronze's append-only contract, this gives full point-in-time reproducibility without explicit snapshot tables.

### 6.5 Force a clean Auto Loader replay (last resort)

Both must happen together:

```python
# 1. Drop the target table
spark.sql("DROP TABLE main.ecom_bronze.events_raw")

# 2. Delete the checkpoint
dbutils.fs.rm("/Volumes/main/ecom_bronze/landing/_checkpoints/events", recurse=True)
```

Then re-run `01_create_tables` and `20_bronze`. **This re-ingests every file in the landing folder.** Only do this if the schema location is corrupted or the table needs to be rebuilt with different properties.

---

## 7 · Operational checklists

### Daily
- Workflow run finished green (Workflows → ecom-medallion → last run).
- `99_quality_checks` ran with no `fail`-severity violations.
- Bronze `_rescued_data` count stable (alert on growth).

### Weekly
- `OPTIMIZE` runs producing reasonable file counts (no fragmentation).
- Storage growth roughly linear with event volume.
- Cluster cost within budget (Workflow run minutes × cluster cost).

### Before promoting a code change
- `ruff check src tests` clean.
- `pytest --cov=src` green and coverage ≥ 60 %.
- `config-validation` job green on the PR.
- Reviewed against [`silver_layer.md`](./silver_layer.md) / [`gold_layer.md`](./gold_layer.md) for invariant violations.

---

## 8 · Where to go next

- Architecture reasoning → [`architecture.md`](./architecture.md)
- Layer logic → [`bronze_layer.md`](./bronze_layer.md), [`silver_layer.md`](./silver_layer.md), [`gold_layer.md`](./gold_layer.md)
- Schemas / ER → [`data_model.md`](./data_model.md)
- Quality framework → [`data_quality.md`](./data_quality.md)
- Test strategy / deploy → [`ci_cd.md`](./ci_cd.md)
