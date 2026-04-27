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
- `[tool.coverage.run].omit` excludes PySpark-bound modules (`src/bronze/*`, `src/silver/*`, `src/gold/*`, `src/common/io.py`, `src/common/schemas.py`). They cannot be imported in this lane (no PySpark installed) and would otherwise pull the total to 0 % for those files. They will be covered separately by the planned `spark-tests` lane.

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

## 8 · Deployment strategy

CI **and** dev CD are implemented. Production CD is the only remaining roadmap item.

### 8.1 Databricks Asset Bundle (`databricks.yml`)

The bundle at the repo root declares the whole medallion DAG and two targets:

```
bundle:
  name: ecom_lakehouse

resources.jobs.medallion.tasks:
  setup → create_tables → simulate → bronze → silver → gold → quality

targets:
  dev:   { mode: development, variables: { catalog: dev_main } }
  prod:  { mode: production,  variables: { catalog: main } }
```

Schedule is **PAUSED** in the bundle definition; flip to `UNPAUSED` and redeploy when you want hourly auto-runs in the workspace.

### 8.2 GitHub Actions deploy job (implemented — `.github/workflows/cd.yml`)

```yaml
name: CD — deploy to dev

on:
  push:
    branches: [main]
  workflow_dispatch:

concurrency:
  group: cd-dev
  cancel-in-progress: false   # never cancel an in-flight deploy

permissions:
  contents: read

jobs:
  deploy-dev:
    runs-on: ubuntu-latest
    environment: dev
    env:
      DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_DEV }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate --target dev
      - run: databricks bundle deploy   --target dev
      - run: databricks bundle run      --target dev medallion
```

#### What each step does

| Step | Behaviour | Failure mode |
|---|---|---|
| `databricks/setup-cli@main` | Installs the Databricks CLI on the runner | runner-side install failure |
| `bundle validate --target dev` | Offline syntax + reference check | typo in `databricks.yml`; bad variable interpolation |
| `bundle deploy --target dev` | Idempotent. Uploads notebooks, updates the Job definition, leaves schedule PAUSED | workspace permission denied; bad token; auth host mismatch |
| `bundle run --target dev medallion` | Synchronous run; bubbles task failures up to GitHub Actions | task-level errors surface as workflow failures |
| `bundle summary --target dev` | (`if: always()`) prints the deployed resources for log forensics | — |

#### Why these choices

- **`environment: dev`** — secrets are scoped to a GitHub Environment, audited, and (optionally) gated behind reviewers. Removes the temptation to use repo-wide secrets that bleed across environments.
- **`concurrency.cancel-in-progress: false`** — cancelling a deploy mid-flight can leave the workspace half-deployed. Worth waiting.
- **No PR trigger** — PRs run CI (`ci.yml`) only. Deploys happen after merge, when `main` is canonical. PR contributors never have credentials to a real workspace.
- **No matrix** — one runner, one target. The CI matrix exists to catch Python-version drift; deploys are environment-specific.

### 8.3 Required GitHub Secrets

Configured in **Repo → Settings → Environments → `dev`**:

| Secret | What it is |
|---|---|
| `DATABRICKS_HOST_DEV`  | dev workspace URL (e.g. `https://dbc-xxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN_DEV` | OAuth M2M token for a service principal scoped to dev |

Use OAuth M2M tokens (not personal access tokens) — they're scoped, rotatable, and not tied to a person who might leave.

### 8.4 Promotion strategy

| Stage | Trigger | Workflow |
|---|---|---|
| Dev | every push to `main` | `cd.yml` (this file) |
| Prod | tagged release (`v*.*.*`) | **planned** — separate `cd-prod.yml`, gated on a protected `prod` Environment with required reviewer + wait timer |

`dev` → `prod` is a target switch in `databricks bundle deploy`. The same artefacts — same notebooks, same `src/` modules — are deployed to both. Per-environment config differs only via the `targets:` block (catalog name, cluster size, optional service principal).

### 8.5 Why DAB over notebook-by-notebook deploy

DAB declares **the whole job topology** in code, version-controlled, reviewable. It deploys notebooks, jobs, schedules, and (when declared) cluster definitions in one transaction. Manual notebook copy/paste between workspaces drifts; DAB does not.

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
| Prod CD deferred behind tag-gated workflow | Auto-deploy prod on every merge | Tagging is intentional friction; prod deploys never happen on every PR-merge |
