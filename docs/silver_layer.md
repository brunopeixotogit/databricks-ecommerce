# Silver Layer

The Silver layer is where the lakehouse stops being a replay log and starts being a model. Bronze is faithful; Silver is **trusted**. Bronze accepts; Silver enforces. This is the layer downstream consumers — Gold marts, ML feature jobs, ad-hoc analysis — actually depend on.

Code: `src/silver/{events,dim_users,dim_products,fact_orders}.py`, orchestrated by `notebooks/30_silver.py`.

---

## 1 · What Silver builds

| Table | Built from | Write strategy | Grain |
|---|---|---|---|
| `silver.events` | `bronze.events_raw` | `MERGE` on `event_id` | one row per event |
| `silver.dim_users_scd2` | `bronze.users_raw` | `overwrite` (full rebuild) | one row per user × version |
| `silver.dim_products_scd2` | `bronze.products_raw` | `overwrite` (full rebuild) | one row per product × version |
| `silver.fact_orders` | `silver.events` (purchases) | `overwrite` partitioned by `order_date` | one row per `order_id` |

After writes, `OPTIMIZE … ZORDER BY` is run on the hot tables (`events`, `fact_orders`) — see § 7.

---

## 2 · `silver.events` — type-clean, dedup, sessionise

```python
VALID_EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "abandon_cart"]

def build_silver_events(spark, bronze, target, inactivity_minutes=30):
    df = (spark.read.table(bronze.fqn)
          .filter(F.col("event_id").isNotNull())
          .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
          .filter(F.col("event_ts").isNotNull()))

    # 1) dedup
    w_dedup = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("_rk", F.row_number().over(w_dedup)).filter("_rk = 1").drop("_rk")

    # 2) sessionize
    w_user = Window.partitionBy("user_id").orderBy("event_ts")
    df = (df.withColumn("_prev_ts", F.lag("event_ts").over(w_user))
            .withColumn("_gap_min",
                (F.col("event_ts").cast("long") - F.col("_prev_ts").cast("long")) / 60.0)
            .withColumn("_new_session",
                F.when(F.col("_prev_ts").isNull() | (F.col("_gap_min") > inactivity_minutes), 1)
                 .otherwise(0))
            .withColumn("_seq", F.sum("_new_session").over(w_user))
            .withColumn("session_id_silver",
                F.concat_ws("_", F.coalesce(F.col("user_id"), F.lit("anon")),
                                 F.col("_seq").cast("string")))
            .withColumnRenamed("session_id", "session_id_raw")
            .withColumn("event_date", F.to_date("event_ts"))
            .drop("_prev_ts", "_gap_min", "_new_session", "_seq"))

    upsert(spark, target, df, keys=["event_id"])
```

### 2.1 Domain filtering

Silver filters out rows that violate the **business contract**: nulls on identity columns, unknown event types. Bronze accepts those rows (it's append-only and tolerant by design); Silver rejects them.

**Why:** downstream joins assume `event_id IS NOT NULL`. Pushing this invariant down to Silver means every Gold mart can rely on it without re-checking.

### 2.2 Deduplication

The dedup window keeps the **latest ingest** per `event_id`:

```python
Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
```

Why latest? Because if an event is re-emitted (CDC update, producer retry with corrected fields), the *newer* version is the canonical one. The older version stays in Bronze for forensic purposes; Silver shows the corrected truth.

### 2.3 Sessionisation — re-derived from event-time gaps

```python
session_id_silver = <user_id|anon>_<seq>
seq = cumulative_count(new_session_marker) over (user_id ordered by event_ts)
new_session_marker = 1 if (prev_ts is null OR gap > inactivity_minutes) else 0
```

The producer's `session_id` is preserved as `session_id_raw` for debugging but **not** used downstream.

#### Why re-derive

- In a real Kafka topology, a user's events may be sharded across partitions. The producer's `session_id` is allocated per-shard and won't be consistent if events arrive out of order across shards.
- Late-arriving events would be assigned to the wrong session if we trusted the producer's value.
- Deriving from `event_ts` alone makes the session ID **deterministic and shuffle-safe**: replay produces the same IDs.

#### Tradeoffs

- The re-derived ID isn't visible at the event source — only after Silver. Real-time use cases that need session boundaries before Silver have to compute them client-side.
- The `30-minute` gap is a global constant. In practice, mobile sessions are shorter than desktop sessions; a per-device gap would be more accurate. We chose simplicity.

### 2.4 Idempotent write — Delta `MERGE`

```python
upsert(spark, target, df, keys=["event_id"])  # src.common.io.upsert
```

Under the hood:
```python
DeltaTable.forName(spark, target.fqn).alias("t")
    .merge(source.alias("s"), "t.event_id = s.event_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
```

#### Why `MERGE` instead of `overwrite`

- Bronze `events_raw` grows monotonically. A full re-read is fine for now (10–100 M rows) but won't scale to a billion.
- `MERGE` lets us reprocess overlapping windows safely. If we re-run Silver against the last 7 days of Bronze, only changed `event_id`s are touched.
- The natural key (`event_id`) is unique by construction; `MERGE` enforces that uniqueness in the target.

#### Tradeoff

`MERGE` is more expensive per row than `INSERT`. We mitigate this with `OPTIMIZE … ZORDER BY (event_ts, user_id, session_id_silver)` — the join key isn't ZORDER'd directly, but the predicate columns Silver reads when re-running an incremental window are.

---

## 3 · `silver.dim_users_scd2` — SCD Type 2 by attribute hash

```python
TRACKED = ["email", "country", "city", "marketing_opt_in", "loyalty_tier"]

hashed = bronze_df.withColumn(
    "_attr_hash",
    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in TRACKED]), 256),
)

w = Window.partitionBy("user_id").orderBy("updated_ts")
boundaries = (hashed
    .withColumn("_prev_hash", F.lag("_attr_hash").over(w))
    .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_attr_hash"))))

versions = (boundaries
    .withColumn("valid_from", F.col("updated_ts"))
    .withColumn("valid_to",   F.lead("updated_ts").over(w))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("version",    F.row_number().over(w)))
```

### 3.1 Hash strategy — only tracked columns, not `updated_ts`

This is the single most important detail. **`updated_ts` is excluded from the hash on purpose.**

The producer's `updated_ts` advances on every snapshot — even when the payload didn't actually change. If we hashed it, every snapshot would open a new SCD2 version, and the dimension would balloon to one row per snapshot instead of one row per *real* change.

By hashing only the tracked attributes, we get the correct semantics: **a new SCD2 version only when something the business cares about actually changed.**

### 3.2 Boundary detection via `lag`

```python
.withColumn("_prev_hash", F.lag("_attr_hash").over(w))
.filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_attr_hash")))
```

This keeps only rows where the hash differs from the previous snapshot. The result is one row per change boundary.

### 3.3 Closed-open intervals

```
valid_from = updated_ts                       (inclusive)
valid_to   = lead(updated_ts) over w          (exclusive; null = still current)
is_current = valid_to IS NULL
version    = row_number() over w              (1-based)
```

Point-in-time joins are trivial:

```sql
SELECT *
FROM   silver.fact_orders o
JOIN   silver.dim_users_scd2 u
  ON   o.user_id  = u.user_id
 AND   o.order_ts BETWEEN u.valid_from
                       AND COALESCE(u.valid_to, '9999-12-31')
```

### 3.4 Why `overwrite` (full rebuild)

Dimension is small (10K users; ~30K versions over time). Full rebuild costs little and avoids `MERGE`'s complexity for SCD2. Bronze is the durable replay log, so we can rebuild Silver any time.

---

## 4 · `silver.dim_products_scd2`

Same shape as users with:

```python
TRACKED = ["name", "category", "subcategory", "brand", "price", "currency", "active"]
```

Tracking **price** is critical for retail: a Q1 sale must be attributed to the price *at the time of sale*, not today's price. This is exactly what SCD2 enables — see the join example above with `order_ts BETWEEN valid_from AND valid_to`.

---

## 5 · `silver.fact_orders` — collapse purchase events into orders

A purchase generates one event per line item. Silver collapses those into one row per `order_id` with a struct array of items, plus computed totals.

```python
purchases = events.filter("event_type = 'purchase' AND order_id IS NOT NULL")

grouped = (purchases.groupBy(
        "order_id", "user_id", "session_id_silver", "country", "payment_method")
    .agg(
        F.min("event_ts").alias("order_ts"),
        F.collect_list(F.struct(
            F.col("product_id"),
            F.col("category"),
            F.col("quantity"),
            F.col("price").alias("unit_price"),
            (F.col("price") * F.col("quantity")).alias("line_total"),
        )).alias("items"),
        F.sum(F.col("price") * F.col("quantity")).alias("subtotal"),
        F.sum("quantity").alias("total_units"))
    .withColumn("status",   F.lit("confirmed"))
    .withColumn("currency", F.lit("USD"))
    .withColumn("tax",      F.round(F.col("subtotal") * F.lit(tax_rate), 2))
    .withColumn("shipping",
        F.when(F.col("subtotal") > F.lit(75.0), F.lit(0.0)).otherwise(F.lit(9.99)))
    .withColumn("total",    F.round(F.col("subtotal") + F.col("tax") + F.col("shipping"), 2))
    .withColumn("order_date", F.to_date("order_ts")))

overwrite(grouped, target, partition_by=["order_date"])
```

### 5.1 Why grain change matters

In Bronze, an order with three line items is **three rows**. Every Gold dashboard would have to remember to `groupBy(order_id)` first to avoid double-counting. Doing the collapse once in Silver means every consumer reads order-grain data correctly without ceremony.

### 5.2 Embedded business rules

- **Tax** — flat 8 % of subtotal.
- **Shipping** — free above $75; flat $9.99 otherwise.
- **Currency** — hard-coded `USD` (single-currency simulation).

These are simulated rules; in a real retailer they'd come from the order-management service via CDC. Computing them once in Silver (rather than in every Gold mart) is the centralisation principle.

### 5.3 Partition by `order_date`

Predicate pushdown for time-bounded BI queries. Most dashboards are "last 7 / 30 / 90 days" — partitioning means those queries scan exactly the relevant files.

---

## 6 · Idempotency summary

| Job | Mechanism | Re-run safe? |
|---|---|---|
| `silver.events` | Delta `MERGE` on `event_id` | yes — overlapping Bronze window cannot duplicate |
| `silver.dim_users_scd2` | hash boundary + `overwrite` | yes — unchanged snapshots produce no new version |
| `silver.dim_products_scd2` | hash boundary + `overwrite` | yes — same as users |
| `silver.fact_orders` | `overwrite` partitioned by `order_date` | yes — full rebuild from current Silver events |

Re-running the entire `30_silver` notebook over an unchanged Bronze produces a Silver byte-equivalent (modulo `OPTIMIZE` history). Combined with Bronze's append-only contract, this gives full reproducibility.

---

## 7 · Performance — `OPTIMIZE … ZORDER BY`

After every Silver build, the notebook runs:

```python
optimize(spark, TableRef(C, S, "events"),
         zorder=["event_ts", "user_id", "session_id_silver"])
optimize(spark, TableRef(C, S, "fact_orders"),
         zorder=["order_ts", "user_id"])
```

ZORDER co-locates rows that share predicate values into the same file, which lets Delta's data-skipping prune most files for filtered queries. The columns are chosen to match the actual BI predicates — `event_ts` for time-bounded dashboards, `user_id` for user-level drill-downs, `session_id_silver` for funnel analysis.

**ZORDER is never applied to partition columns** (`event_date` on `silver.events` would raise `DELTA_ZORDERING_ON_PARTITION_COLUMN`). Partitioning already provides skip on the partition column.

---

## 8 · Tradeoffs and engineering choices

| Choice | Alternative we rejected | Why |
|---|---|---|
| Re-derive `session_id_silver` | Trust the producer's `session_id` | Shuffle-safety; deterministic from `event_ts` |
| Hash only tracked attributes | Hash whole row (incl. `updated_ts`) | Avoids spurious SCD2 versions on identical snapshots |
| `MERGE` for events, `overwrite` for SCD2/facts | One pattern everywhere | Right tool per case — events are large/incremental, SCD2 is small/regenerable |
| Filter invalid rows in Silver | Filter at Bronze | Bronze must stay tolerant; rejection is a Silver concern |
| Tax/shipping rules in Silver `fact_orders` | Push to Gold | Centralises business rules — every mart agrees on the same `total` |
| Partition `fact_orders` by `order_date` | Partition by `country` or `category` | Time-range filters dominate BI workloads |

---

## 9 · Common Silver failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Silver `events` row count exceeds Bronze | Dedup window keys are wrong (e.g. `event_id` is null for some rows) | The filter `F.col("event_id").isNotNull()` runs **before** dedup — verify Bronze has non-null `event_id` |
| SCD2 dimension explodes to one row per snapshot | `_attr_hash` computed including `updated_ts` | Confirm `TRACKED_COLS` excludes timestamps |
| `fact_orders.subtotal != sum(items.line_total)` | Producer emitted same `order_id` across multiple sessions | Check Bronze for ambiguous `order_id`; the groupBy collapses across `session_id_silver` already |
| MERGE conflicts during concurrent runs | Two `30_silver` runs overlap | Don't run concurrent Silver. Workflows enforce serial execution. |

See [`runbook.md`](./runbook.md) for the full operational playbook.
