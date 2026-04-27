# Gold Layer

Gold is where the lakehouse meets the consumer. Every table in Gold is **denormalised, query-tuned, and consumer-shaped** — a dashboard, a marketing tool, a CRM segment, an ML feature table all read directly from here without further joins or aggregations.

Code: `src/gold/{daily_sales,funnel,abandoned_carts,user_360}.py`, orchestrated by `notebooks/40_gold.py`.

---

## 1 · The four marts

| Table | Grain | Type | Powers |
|---|---|---|---|
| `fact_daily_sales`     | day × category × country | Fact (additive) | Sales / GMV / AOV dashboards |
| `fact_funnel`          | day                      | Fact (rate-based) | View → cart → purchase conversion dashboards |
| `fact_abandoned_carts` | session                  | Fact (event-grain) | Email & ad remarketing campaigns |
| `dim_user_360`         | user (current SCD2 version) | Dimension | RFM segmentation; ML feature base |

All four are written via `overwrite` with `overwriteSchema=true`. Gold can be rebuilt at any time from Silver — see § 6 for why.

---

## 2 · `fact_daily_sales` — sales rollup

```python
line = (spark.read.table(fact_orders.fqn)
    .select(F.to_date("order_ts").alias("sale_date"),
            "country", "order_id", "total",
            F.explode("items").alias("item"))
    .select("sale_date", "country", "order_id", "total",
            F.col("item.category").alias("category"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.line_total").alias("line_total")))

daily = (line.groupBy("sale_date", "category", "country").agg(
    F.countDistinct("order_id").alias("orders"),
    F.sum("quantity").alias("units"),
    F.round(F.sum("line_total"), 2).alias("gmv"),
    F.round(F.avg("total"), 2).alias("avg_order_value")))

overwrite(daily, target, partition_by=["sale_date"])
```

### Grain choice — day × category × country

This is the **smallest cube** that supports the canonical questions:
- *"What was GMV for electronics in BR yesterday?"*
- *"How does AOV compare across categories week-over-week?"*

Slicing by category and country at this stage means BI tools never re-explode the `items` array. A finer grain (e.g. day × product × country) would be 100× larger without proportional analytical value — the assortment strategy lives in `dim_products_scd2`, not here.

### KPIs

| KPI | Formula | Why |
|---|---|---|
| `orders` | `COUNT(DISTINCT order_id)` | Distinct because an order spans multiple `items` lines after `explode` |
| `units` | `SUM(item.quantity)` | Item-level units sold |
| `gmv` | `SUM(item.line_total)` | Gross merchandise value at the line-item level |
| `avg_order_value` | `AVG(order.total)` | Order-level (not line-level) — uses `total` from `fact_orders` |

### Partitioning + ZORDER

```yaml
optimize:
  zorder:
    gold.fact_daily_sales: [category, country]
```

Partitioned by `sale_date`. **ZORDER never includes `sale_date`** — Delta raises `DELTA_ZORDERING_ON_PARTITION_COLUMN` (SQLSTATE 42P10), and partitioning already provides skipping on that column. The ZORDER targets are the columns BI predicates filter on after the date range is fixed.

---

## 3 · `fact_funnel` — session-grain conversion rates

```python
sessions = (events
    .groupBy(F.to_date("event_ts").alias("event_date"), "session_id_silver")
    .agg(
        F.max((F.col("event_type") == "page_view").cast("int")).alias("viewed"),
        F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("carted"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased"),
        F.max((F.col("event_type") == "abandon_cart").cast("int")).alias("abandoned"),
    ))

funnel = (sessions.groupBy("event_date").agg(
        F.sum("viewed").alias("sessions_viewed"),
        F.sum("carted").alias("sessions_carted"),
        F.sum("purchased").alias("sessions_purchased"),
        F.sum("abandoned").alias("sessions_abandoned"))
    .withColumn("cart_rate",
        F.when(F.col("sessions_viewed") > 0,
               F.col("sessions_carted") / F.col("sessions_viewed")))
    .withColumn("purchase_rate",
        F.when(F.col("sessions_viewed") > 0,
               F.col("sessions_purchased") / F.col("sessions_viewed")))
    .withColumn("abandon_rate",
        F.when(F.col("sessions_carted") > 0,
               F.col("sessions_abandoned") / F.col("sessions_carted"))))
```

### Why session grain (not event grain)

A session is the natural unit of **purchase intent**. Computing `cart_rate` at event grain (events with carting / events with viewing) is meaningless — one user with 50 page views and 1 cart shouldn't dilute the rate of users who view once and buy.

The `MAX(event_type == 'X')` per session is a classic "did this happen at all in the session?" pattern — gives boolean flags that aggregate cleanly to daily counts.

### Why store rates in addition to counts

Dashboards could compute the rates on read. We pre-compute because:
1. Three different BI tools shouldn't reimplement the same arithmetic.
2. The division-by-zero handling (`WHEN sessions_viewed > 0`) lives once.
3. Rates are queried more often than counts; pre-computing pays off.

### Tradeoff

The mart double-stores information (counts and rates). On a 365-row-per-year table that's noise; if the mart got bigger, we'd pick one and document the math.

---

## 4 · `fact_abandoned_carts` — remarketing candidates

```python
cart_value = (events.filter("event_type = 'add_to_cart'")
    .groupBy("session_id_silver", "user_id", "cart_id", "country")
    .agg(F.min("event_ts").alias("cart_started_ts"),
         F.max("event_ts").alias("last_cart_event_ts"),
         F.round(F.sum(F.col("price") * F.col("quantity")), 2).alias("cart_value"),
         F.sum("quantity").alias("items_in_cart"),
         F.collect_set("category").alias("categories")))

purchases = events.filter("event_type = 'purchase'").select("session_id_silver").distinct()

abandoned = (cart_value
    .join(purchases, "session_id_silver", "left_anti")
    .withColumn("cart_date", F.to_date("cart_started_ts"))
    .withColumn("remarketable", F.col("user_id").isNotNull()))
```

### Definition of "abandoned"

A session contains `add_to_cart` events but **no** `purchase`. The `LEFT ANTI JOIN` against purchasing sessions is the cleanest expression of "subtract the buyers."

### Why `user_id IS NOT NULL` flag

`remarketable = true` means we have an account — therefore an email and a marketing-opt-in flag — so the cart can power an email or SMS retargeting flow. Anonymous abandons (`user_id IS NULL`) only support cookie-based ad retargeting, a different campaign bucket. The flag lets the marketing tool route accordingly without re-deriving the predicate.

### Cart value vs. final order value

Cart value is `SUM(add_to_cart.price * quantity)` — a snapshot of the *intent* at carting time, before the user removes items, applies coupons, or hits a tax/shipping line. We deliberately do not project order-side rules here because the user never reached checkout; the question is "what did they want?", not "what would they have paid?"

### Partition by `cart_date`

Same rationale as `fact_daily_sales`: most queries are time-bounded.

---

## 5 · `dim_user_360` — per-user lifetime metrics

```python
users   = spark.read.table(dim_users.fqn).filter("is_current = true")
orders  = spark.read.table(fact_orders.fqn)
events  = spark.read.table(silver_events.fqn)

om = orders.groupBy("user_id").agg(
    F.count("order_id").alias("orders_count"),
    F.round(F.sum("total"), 2).alias("ltv"),
    F.round(F.avg("total"), 2).alias("avg_order_value"),
    F.max("order_ts").alias("last_order_ts"),
    F.min("order_ts").alias("first_order_ts"))

act = events.groupBy("user_id").agg(
    F.countDistinct("session_id_silver").alias("sessions_count"),
    F.max("event_ts").alias("last_seen_ts"))

out = (users.join(om, "user_id", "left").join(act, "user_id", "left")
    .withColumn("recency_days",
        F.when(F.col("last_order_ts").isNotNull(),
               F.datediff(F.current_date(), F.to_date("last_order_ts"))))
    .withColumn("segment",
        F.when(F.col("orders_count").isNull(),  F.lit("prospect"))
         .when(F.col("recency_days") <= 30,     F.lit("active"))
         .when(F.col("recency_days") <= 180,    F.lit("lapsing"))
         .otherwise(F.lit("churned"))))
```

### Joins to **current** SCD2 version only

```python
users = spark.read.table(dim_users.fqn).filter("is_current = true")
```

`dim_user_360` is a snapshot of *who the user is right now*. Historical state lives in `dim_users_scd2`; if you want to know what segment the user was in three months ago, you join `fact_orders.order_ts BETWEEN valid_from AND valid_to` against the SCD2 table.

### KPIs — RFM-style

| KPI | Calc | Use |
|---|---|---|
| `orders_count` | `COUNT(order_id)` | Frequency |
| `ltv` | `SUM(total)` | Monetary (lifetime value) |
| `recency_days` | `DATEDIFF(current_date, last_order_ts)` | Recency |
| `avg_order_value` | `AVG(total)` | Spend signal |
| `sessions_count` | `COUNT(DISTINCT session_id_silver)` | Engagement |

### Lifecycle segment logic

```
prospect  ← no orders ever
active    ← last order ≤ 30 days
lapsing   ← last order ≤ 180 days
churned   ← otherwise
```

Why these thresholds? Industry-typical for general retail. They're not in `conf/`; they're business definitions co-owned with the marketing team — but they're trivially extractable to YAML if marketing wants to A/B-test the boundaries.

### Why outer joins (`how="left"`)

Some users have signed up but never ordered (prospects). `INNER JOIN` would drop them and we'd lose the `prospect` segment entirely — which is exactly the segment the marketing tool needs for activation campaigns.

---

## 6 · BI readiness checklist

Each mart satisfies this checklist:

| Property | Why it matters |
|---|---|
| Denormalised | One read = one chart. No `JOIN` needed in the BI tool. |
| Pre-aggregated to its natural grain | Dashboard latency stays in the sub-second range. |
| Partitioned by date column | Time-range filters are the dominant predicate; partition pruning makes them fast. |
| ZORDER'd on filter columns | Multi-dimensional filters (category × country) skip irrelevant files. |
| Pre-computed rates | Three BI tools don't reimplement the same `CASE WHEN` divisor. |
| Stable schema | Dashboard authors can rely on column names not flickering. |
| Idempotent rebuild | Backfills are safe; nightly refresh just works. |

---

## 7 · Why `overwrite` for Gold (not `MERGE`)

- Gold marts are small relative to Silver (orders of millions, not billions).
- They're cheap to rebuild from Silver, which is the contract source of truth.
- `overwriteSchema=true` lets us add a column to a mart without a migration.
- Avoids `MERGE`'s concurrency complications — Gold always reflects the latest Silver state, never a partial merge.

The principle: **make the layer you can rebuild idempotent by full rebuild; make the layer you can't rebuild idempotent by `MERGE`.** Gold is rebuildable; Silver `events` is harder (relies on Bronze, also rebuildable, but more rows).

---

## 8 · Querying Gold from BI

Examples a Tableau / PowerBI / Lightdash author would actually write:

```sql
-- Last 30 days GMV by category
SELECT category, SUM(gmv) AS gmv_30d
FROM   main.ecom_gold.fact_daily_sales
WHERE  sale_date >= current_date() - INTERVAL 30 DAYS
GROUP  BY category
ORDER  BY gmv_30d DESC;

-- Conversion rate trend
SELECT event_date, purchase_rate
FROM   main.ecom_gold.fact_funnel
WHERE  event_date >= current_date() - INTERVAL 90 DAYS
ORDER  BY event_date;

-- Remarketing universe
SELECT u.email, c.cart_value, c.categories, c.cart_started_ts
FROM   main.ecom_gold.fact_abandoned_carts c
JOIN   main.ecom_gold.dim_user_360 u USING (user_id)
WHERE  c.remarketable = true
  AND  c.cart_started_ts >= current_date() - INTERVAL 7 DAYS
  AND  u.segment IN ('active', 'lapsing');

-- High-value lapsing users for win-back
SELECT user_id, ltv, last_order_ts
FROM   main.ecom_gold.dim_user_360
WHERE  segment = 'lapsing'
  AND  ltv > 1000
ORDER  BY ltv DESC
LIMIT  500;
```

---

## 9 · Tradeoffs and choices

| Choice | Alternative | Why |
|---|---|---|
| Pre-aggregate to day grain in `fact_daily_sales` | Aggregate on read | BI latency; one source of truth for KPIs |
| Store both counts and rates in `fact_funnel` | Compute rates on read | Dashboard simplicity, single divisor handling |
| Snapshot current SCD2 in `dim_user_360` | Always join SCD2 at read | `dim_user_360` is consumer-facing; consumers don't want to bring SCD2 logic |
| `overwrite` Gold | `MERGE` Gold | Gold is rebuildable; simpler, faster, no merge conflicts |
| Hard-code lifecycle thresholds (30/180 days) | Pull from `conf/` | These are business definitions; extract when marketing wants to tune |
| Embed remarketability flag in `fact_abandoned_carts` | Re-derive per dashboard | Same predicate, three places — bad. Centralise. |
