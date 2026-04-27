# CI/CD

How the project is tested, validated, and (in the future) deployed. The CI side is implemented today; the CD side outlines the planned roadmap with Databricks Asset Bundles.

Code: `.github/workflows/ci.yml`, `pyproject.toml`, `tests/`.

---

## 1 · Testing philosophy

Three explicit goals shape the CI design:

1. **Fast feedback.** A typical PR finishes lint + tests in under 90 seconds.
2. **No false dependence on Databricks.** All required CI tooling runs on a vanilla Ubuntu runner. No workspace, no token, no cluster.
3. **Tests trust the code, not the fixtures.** Pure-Python tests exercise real `src/*` modules — no parallel "test versions" of business logic.

These goals push three concrete decisions:
- **PySpark is opt-in, not default.** Heavy install (~300 MB), slow import (~5 s). Most logic doesn't need it.
- **Notebooks are NOT in CI.** They're thin shells over `src/`; testing the source covers the logic without spinning up Databricks.
- **Config validation is a separate job.** A YAML typo shouldn't have to wait for the test matrix.

---

## 2 · Workflow file (`.github/workflows/ci.yml`)

Three independent jobs, runs on push and PR to `main` plus `workflow_dispatch`:

```
push / PR / manual ──► concurrency dedupe (cancel-in-progress)
                          │
        ┌─────────────────┼──────────────────────────┐
        │                 │                          │
        ▼                 ▼                          ▼
┌──────────────────┐  ┌──────────────────┐  ┌────────────────┐
│ lint-and-test    │  │ config-validation│  │ secrets-scan   │
│ (3.10/3.11/3.12) │  │                  │  │  (gitleaks)    │
│  pip install     │  │  load each       │  │                │
│       -e .[dev]  │  │  conf/*.yml      │  │  full git      │
│  import smoke    │  │  via             │  │  history       │
│  ruff check      │  │  src.common.     │  │                │
│  pytest --cov    │  │  config          │  │                │
│  upload coverage │  │                  │  │                │
└──────────────────┘  └──────────────────┘  └────────────────┘
```

`permissions: contents: read` — least privilege. `concurrency` cancels superseded runs on the same ref so a fast follow-up commit doesn't queue behind a stale one.

---

## 3 · Job: `lint-and-test`

Runs against three Python versions (3.10, 3.11, 3.12) in a `fail-fast: false` matrix.

### 3.1 Install
```bash
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

`pip install -e .` makes the project an installed package — `import src.simulator.run` works without `sys.path` hackery. `[dev]` adds `pytest`, `pytest-cov`, `chispa`, `ruff`. **No PySpark** — see § 4.

### 3.2 Import-graph smoke

```python
import importlib
modules = [
    'src.common.version',
    'src.common.config',
    'src.common.quality',
    'src.simulator.entities',
    'src.simulator.behavior',
    'src.simulator.emit',
    'src.simulator.run',
]
# All must import cleanly without PySpark.
```

#### Why this exists separately from `pytest`

It's a tripwire for the kind of breakage that's easy to miss: a renamed module nobody imports yet, a typo in `__init__.py`, an accidental circular import. The smoke runs in milliseconds and surfaces these before the test matrix wastes a runner.

It also enforces an architectural invariant — **pure-Python modules must not transitively import PySpark.** When a contributor accidentally adds `from pyspark.sql import DataFrame` to `src/simulator/`, this job catches it.

### 3.3 Lint
```bash
ruff check src tests
```

Ruff config in `pyproject.toml` — `E/F/I/B/UP/SIM` rule families, line-length 100. Notebooks are excluded (Databricks `# COMMAND ----------` markers and `dbutils` aren't standard Python).

### 3.4 Tests + coverage
```bash
pytest --cov=src --cov-report=xml --cov-report=term
```

- `--cov=src` measures branch coverage on the source tree (not tests themselves).
- `coverage.xml` uploaded as an artifact from the 3.11 cell only (avoids duplicate uploads).
- `fail_under = 60` in `pyproject.toml` — drift below 60 % fails CI.

### 3.5 Why three Python versions

`requires-python = ">=3.10"` is declared in `pyproject.toml`. The matrix proves the floor and ceiling actually work — typing.Self, dataclass kw-only, and PEP 604 union syntax all behave differently across these versions, and a CI matrix surfaces version-specific bugs before users hit them.

---

## 4 · PySpark — opt-in via `[spark]` extra

```toml
[project.optional-dependencies]
spark = [
    "pyspark>=3.5,<4.0",
    "delta-spark>=3.2,<4.0",
]
```

Local Spark tests:
```bash
pip install -e ".[dev,spark]"
pytest -m spark
```

The `spark` pytest fixture in `tests/conftest.py` uses `pytest.importorskip("pyspark")` — when PySpark isn't installed, the test is **skipped, not failed**. This means the same test file works in the default CI lane (Spark tests skip cleanly) and locally with the spark extra (Spark tests actually run).

### Roadmap: `spark-tests` job

A separate workflow file is planned for Spark integration tests, gated on path filters so it only runs when `src/silver/**` or `src/gold/**` changes. Keeps the default CI lane fast and reserves the heavy install for changes that actually need it.

---

## 5 · Job: `config-validation`

```python
from src.common.config import load_config
for name in ("pipeline", "simulator"):
    cfg = load_config(name)
    assert isinstance(cfg, dict) and cfg, f"{name}.yml is empty or invalid"
```

Loads each YAML through the same loader the pipeline uses — including env-override application. A malformed YAML or a regression in the loader fails the job before any pipeline code is touched.

#### Why this is separate from `lint-and-test`

If `pyproject.toml` has a syntax error, the test job fails to install and we never learn if `conf/pipeline.yml` is also broken. Splitting jobs makes failure modes orthogonal — one red doesn't mask another.

---

## 6 · Job: `secrets-scan`

```yaml
- uses: gitleaks/gitleaks-action@v2
  continue-on-error: true
```

Scans the **full git history** (not just the current diff) for accidentally-committed credentials. `continue-on-error: true` is intentional — false positives shouldn't block a merge, but real findings are visible in the workflow log.

### Defense-in-depth, not the only line

`.gitignore` denylists `.env*`, `*.pem`, `*.key`, `secrets/`. CODEOWNERS-style review on `conf/**` would be a sensible addition for a team with more than one contributor.

---

## 7 · Git workflow

### Branch model
- **`main`** is the protected, deployable branch.
- Feature branches off `main`; merge via PR.
- Commits use conventional-ish messages (`feat:`, `fix:`, `chore:`, `refactor:`, `docs:`).
- Force-pushes to `main` are forbidden.

### Recommended branch protection (GitHub UI)
- Require `lint-and-test (3.11)` and `config-validation` to be green.
- Require linear history.
- Require code review for changes touching `src/silver/**`, `src/gold/**`, `conf/**`.

These aren't enforced in the repo (a CLI tool can't set branch protection); document them in the team handbook.

---

## 8 · Deployment strategy (planned — not yet implemented)

The CI side is implemented. The CD side is a roadmap for when this graduates from Free Edition to a paid Databricks workspace.

### 8.1 Databricks Asset Bundles (DAB)

Add `databricks.yml` at the repo root declaring:

```yaml
bundle:
  name: ecom_lakehouse

resources:
  jobs:
    medallion:
      name: ecom-medallion
      tasks:
        - task_key: setup
          notebook_task: { notebook_path: notebooks/00_setup.py }
        - task_key: create_tables
          depends_on: [{ task_key: setup }]
          notebook_task: { notebook_path: notebooks/01_create_tables.py }
        # ... 10_run_simulator → 20_bronze → 30_silver → 40_gold → 99_quality_checks
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: UTC

targets:
  dev:
    workspace: { host: https://<dev-host>.cloud.databricks.com }
    variables: { catalog: dev_main }
  prod:
    workspace: { host: https://<prod-host>.cloud.databricks.com }
    variables: { catalog: main }
```

### 8.2 GitHub Actions deploy job (planned)

```yaml
deploy:
  needs: [lint-and-test, config-validation]
  if: github.ref == 'refs/heads/main' && github.event_name == 'push'
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: databricks/setup-cli@main
    - run: databricks bundle deploy --target prod
      env:
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

### 8.3 Promotion strategy

`dev` → `prod` is a target switch in `databricks bundle deploy`. The same artifacts — same notebooks, same `src/` modules — are deployed to both. Per-environment config differs only via the `targets:` block (catalog name, cluster size).

#### Why DAB over notebook-by-notebook deploy

DAB declares **the whole job topology** in code, version-controlled, reviewable. It deploys notebooks, jobs, schedules, and even cluster definitions in one transaction. Manual notebook copy/paste between workspaces drifts; DAB does not.

---

## 9 · Pipeline orchestration (within Databricks)

A single Workflow runs the medallion DAG:

```
00_setup ─► 01_create_tables ─► 10_run_simulator ─► 20_bronze
       ─► 30_silver ─► 40_gold ─► 99_quality_checks
```

| Concern | Mechanism |
|---|---|
| Triggering | Cron via Workflow schedule (e.g. hourly off-peak) |
| Dependencies | DAG edges in Workflow definition (or `depends_on:` in DAB) |
| Failure handling | `99_quality_checks` raises → run aborts; alerts via Workflow notifications |
| Retries | Workflow-level retry policy (default 0 — failures are usually deterministic; opt in per task) |
| Cluster lifecycle | `availableNow` triggers + auto-terminate cluster (Free Edition: shared cluster, no per-job spin-up) |

---

## 10 · Tradeoffs

| Choice | Alternative we rejected | Why |
|---|---|---|
| Pure-Python CI by default | Always install PySpark | 300 MB / 5-second import per matrix cell × 3 versions = significant CI time |
| Notebooks excluded from CI | Lint/test notebooks too | Notebooks have Databricks-specific syntax; lint complains; integration belongs in DAB-driven workspace tests |
| `gitleaks` as advisory | Block merge on findings | False positives would block harmless PRs; treat findings as a triage signal |
| Three Python versions | One pinned version | Floor / ceiling matter; surface version-specific bugs early |
| Coverage floor at 60 % | Strict 100 % | Anti-pattern: gameable; we'd rather grow real coverage than write tests for `__init__.py` |
| Asset Bundles deferred | Implement now | Free Edition doesn't support per-target deploy fully; documented as roadmap when promoted to a paid tier |
