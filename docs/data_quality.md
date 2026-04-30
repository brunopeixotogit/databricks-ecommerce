# Data Quality

Data quality in this project is **declarative, executable, and gating**. Expectations are SQL predicates that should evaluate `TRUE` for every valid row. The same predicates run in **both execution paths**:

- **Workflow path** — `notebooks/99_quality_checks.py` runs the `Expectation` framework as the final task; an `enforce()` failure aborts the Workflow.
- **DLT path** — the same predicates are encoded as `@dlt.expect_or_fail` / `@dlt.expect` decorators directly on the `silver.events` and `silver.fact_orders` table definitions in `pipelines/dlt/silver.py`; an `expect_or_fail` violation fails the DLT update.

Code: `src/common/quality.py`, `notebooks/99_quality_checks.py`, `pipelines/dlt/silver.py`.

---

## 1 · The framework

```python
@dataclass(frozen=True)
class Expectation:
    name: str
    predicate: str            # SQL — TRUE means valid row
    severity: str = "warn"    # 'warn' or 'fail'

def evaluate(df, expectations):
    """Run every expectation, return per-expectation counts and pass-rate."""
    total = df.count()
    return [
        {
            "expectation": e.name,
            "predicate":   e.predicate,
            "severity":    e.severity,
            "total":       total,
            "failing":     (bad := df.filter(f"NOT ({e.predicate})").count()),
            "pass_rate":   (total - bad) / total if total else 1.0,
        }
        for e in expectations
    ]

def enforce(results):
    """Raise if any 'fail'-severity expectation has any failing row."""
    failed = [r for r in results if r["severity"] == "fail" and r["failing"] > 0]
    if failed:
        raise AssertionError(
            "Quality gate failed: "
            + ", ".join(f"{r['expectation']} ({r['failing']}/{r['total']})" for r in failed)
        )
```

### Why declarative SQL predicates

- **Readable by non-engineers.** Analysts and PMs can audit `event_type IN ('page_view', ...)` directly. They cannot audit a Python function.
- **Storage-agnostic.** Same predicate runs on Spark today and could run on Snowflake or BigQuery tomorrow.
- **Composable.** Expectations are values, not functions; they can be loaded from YAML, generated, or filtered without ceremony.

---

## 2 · Validation rules — what we actually check

`notebooks/99_quality_checks.py` defines two expectation suites against Silver. They are the gates that protect Gold.

### 2.1 `silver.events`

```python
event_expectations = [
    Expectation("event_id_not_null",
                "event_id IS NOT NULL", "fail"),
    Expectation("valid_event_type",
                "event_type IN ('page_view','add_to_cart','purchase','abandon_cart')", "fail"),
    Expectation("session_id_silver_present",
                "session_id_silver IS NOT NULL", "fail"),
    Expectation("non_negative_price",
                "price IS NULL OR price >= 0", "warn"),
    Expectation("positive_quantity",
                "quantity IS NULL OR quantity > 0", "warn"),
    Expectation("purchase_has_order_id",
                "event_type <> 'purchase' OR order_id IS NOT NULL", "fail"),
]
```

### 2.2 `silver.fact_orders`

```python
order_expectations = [
    Expectation("order_total_positive",      "total >= 0",          "fail"),
    Expectation("order_subtotal_consistent", "subtotal >= 0",       "fail"),
    Expectation("order_has_items",           "size(items) > 0",     "fail"),
    Expectation("order_total_matches",
                "abs(total - (subtotal + tax + shipping)) < 0.05", "warn"),
]
```

### Why each rule exists

| Rule | Reason |
|---|---|
| `event_id_not_null` | Dedup keys on `event_id`. A null key means we cannot guarantee uniqueness — fail loudly. |
| `valid_event_type` | The four known types drive every downstream transformation. An unknown type means a producer drifted. |
| `session_id_silver_present` | Sessionisation invariant. Anything missing means the Silver build is broken. |
| `purchase_has_order_id` | `fact_orders` groups by `order_id`; a purchase without one would silently disappear. |
| `non_negative_price` / `positive_quantity` | Soft business rules. Bad value? Investigate, but don't block the pipeline. |
| `order_total_positive` / `order_subtotal_consistent` / `order_has_items` | Order-grain integrity — anything failing means `fact_daily_sales` is wrong. |
| `order_total_matches` | `total = subtotal + tax + shipping ± 0.05` (rounding tolerance). Catches drift between Silver's computation and any future change to the rules. |

---

## 3 · Severity levels

Two levels, on purpose:

| Severity | Behaviour | When to use |
|---|---|---|
| `fail` | `enforce()` raises → Workflow run aborts. | Invariants downstream code **assumes**. Failure = bug. |
| `warn` | Counted and logged; pipeline continues. | Soft business rules where outliers are interesting but not blocking. |

#### Why two levels (not three or one)

- **Three (info/warn/fail)** is more configuration than benefit at this scale. The "info" tier in practice degenerates into noise nobody reads.
- **One (everything fails)** is too brittle. The first time a single bad row from a producer aborts the entire pipeline, the team disables the gate, and now you have *no* gate.

Two levels split the world cleanly: invariants vs. signals.

---

## 4 · How failures are handled

```python
event_results = evaluate(events, event_expectations)
order_results = evaluate(orders, order_expectations)

for r in event_results: print(r)
for r in order_results: print(r)

enforce(event_results)
enforce(order_results)
```

### Behaviour by failure type

| What happens | Outcome |
|---|---|
| All `fail` predicates pass | Run continues; Gold marts are valid; downstream consumers see fresh data. |
| Any `fail` predicate has ≥1 violating row | `AssertionError` raised → Workflow run aborts → Gold from this run is **not** published → on-call paged via Workflow notification. |
| `warn` predicate has violating rows | Logged in the cell output; Workflow run continues. Counts visible in the Workflow log for triage. |

### Why placement at the end of the Workflow matters

The gate runs **after** Silver and Gold. If a `fail` predicate raises, Gold has *already been written* — but the Workflow status is `FAILED`, downstream consumers see no green build, and the team triages before the next scheduled run overwrites Gold with a corrected version. This is intentional: rolling back Gold automatically would be more dangerous than leaving it visibly broken in a failed run.

In a stricter setup, you'd run the gate against Silver only and write Gold conditional on the gate passing. That's a reasonable tightening; we chose the simpler version because Gold's `overwrite` semantics make corrective re-runs trivial.

---

## 5 · Anomaly detection (lightweight, not statistical)

The current framework checks **invariants**, not anomalies. There's no statistical detector for "GMV dropped 40 % vs. yesterday" — that lives in the BI dashboard, not the pipeline.

### What we do detect

- **Schema invariants** — pinned `StructType` + `_rescued_data` non-empty alerts.
- **Business invariants** — the predicates above.
- **Volume invariants** — implicit. The Bronze sanity-check cell counts rows; a sudden drop is visible.

### What we don't detect (and why)

- **Distributional shifts.** Catching "the average order value changed" needs a baseline, statistical comparison, and a tunable threshold. Out of scope for a deterministic pipeline; belongs in observability tooling (Monte Carlo, Anomalo, etc.) or a dedicated metric store.
- **Cross-table consistency at scale.** *"Did `fact_daily_sales.gmv` agree with `fact_orders.total`?"* — easy to write as an expectation, but expensive at production volume. Run it weekly, not per-batch.

---

## 6 · Tradeoffs

| Choice | Alternative we rejected | Why |
|---|---|---|
| SQL predicates | Python validators (`def check(df) -> bool`) | Readable by analysts; storage-agnostic; loadable from YAML later |
| Two severity levels | Three (info/warn/fail) | Simpler; "info" tier in practice degenerates into noise |
| Gate at end of pipeline (after Gold write) | Gate before Gold write | Simpler; failed runs are visible; corrective re-runs are easy thanks to `overwrite` |
| Hand-rolled framework (Workflow) | Only DLT expectations | The framework is the only option for the notebook DAG; we now have **both** paths in parallel — see §7 |
| Hand-rolled framework | Great Expectations | Heavier; more dependencies; not worth the weight at this scale |

---

## 7 · DLT expectations — parity with the framework

The DLT pipeline encodes the **same predicates with the same severities** as the Workflow path. Each `Expectation` in `notebooks/99_quality_checks.py` has a one-to-one DLT equivalent in `pipelines/dlt/silver.py`:

| Workflow `Expectation` (severity) | DLT decorator on `silver.events` / `silver.fact_orders` |
|---|---|
| `event_id_not_null` (fail) | `@dlt.expect_or_fail("event_id_not_null", "event_id IS NOT NULL")` |
| `valid_event_type` (fail) | `@dlt.expect_or_fail("valid_event_type", "event_type IN ('page_view','add_to_cart','purchase','abandon_cart')")` |
| `session_id_silver_present` (fail) | `@dlt.expect_or_fail("session_id_silver_present", "session_id_silver IS NOT NULL")` |
| `purchase_has_order_id` (fail) | `@dlt.expect_or_fail("purchase_has_order_id", "event_type <> 'purchase' OR order_id IS NOT NULL")` |
| `non_negative_price` (warn) | `@dlt.expect("non_negative_price", "price IS NULL OR price >= 0")` |
| `positive_quantity` (warn) | `@dlt.expect("positive_quantity", "quantity IS NULL OR quantity > 0")` |
| `order_total_positive` (fail) | `@dlt.expect_or_fail("order_total_positive", "total >= 0")` |
| `order_subtotal_consistent` (fail) | `@dlt.expect_or_fail("order_subtotal_consistent", "subtotal >= 0")` |
| `order_has_items` (fail) | `@dlt.expect_or_fail("order_has_items", "size(items) > 0")` |
| `order_total_matches` (warn) | `@dlt.expect("order_total_matches", "abs(total - (subtotal + tax + shipping)) < 0.05")` |

### Behavioural differences

| Aspect | Workflow framework | DLT expectations |
|---|---|---|
| Where it runs | Final task of the DAG, after Silver + Gold are written | Inline on the table definition; evaluated as rows flow through |
| `fail` semantics | `enforce()` raises; run aborts; Gold from this run remains in place but the run is `FAILED` | The DLT update fails; the table version is rolled back; downstream tables in the same update are skipped |
| `warn` semantics | Counted, logged, no abort | Counted, surfaced in the DLT event log, no abort |
| Visibility | Stdout in the cell + Workflow log | Per-expectation drop counters in the DLT UI + event log |
| Lineage integration | None (it's just a notebook) | Native — expectation metrics tagged with table version |

The vocabulary (predicate + severity) is identical, so a rule added in one path is mechanically portable to the other.

### When predicates diverge

The two paths share the same Bronze landing Volume but write to **different schemas** (Workflow → `ecom_silver`/`ecom_gold`; DLT → `ecom_dlt`). If a predicate must change, update **both** definitions in the same PR; CI verifies neither path drifts in isolation.

---

## 8 · Adding a new expectation

1. Identify the **predicate** in SQL terms — *"every valid row satisfies …"*.
2. Pick **severity** — does downstream code assume this is true? `fail`. Is it a signal you want to monitor without blocking? `warn`.
3. Add an `Expectation(...)` to the relevant suite in `99_quality_checks.py`.
4. Run `30_silver` + `99_quality_checks` locally (or on a dev catalog) to verify the gate fires when violated and passes when not.
5. PR. CI doesn't validate expectations directly (they need Spark); reviewers are the safety net.

### Anti-patterns to avoid

- **Implementation-detail predicates** — *"`_session_seq` is monotonically increasing"*. The fix would change with the sessionisation algorithm; the gate would calcify the implementation.
- **Tautologies** — *"`event_id` matches the regex it was generated with"*. Adds nothing; just slows the suite.
- **Predicates that re-derive Silver logic** — if you find yourself re-implementing `fact_orders` math in an expectation, the test belongs in `tests/`, not as a runtime gate.

The right predicates are **stable, simple, and orthogonal to the implementation**.
