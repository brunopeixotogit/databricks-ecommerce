# Data Model

This document captures the **shape of the data** at every layer — the pinned schemas, the entity relationships, and the modelling decisions that hold the lakehouse together.

Code: `src/common/schemas.py`, `src/common/version.py`.

---

## 1 · Schema versioning contract

```python
# src/common/version.py
SCHEMA_VERSION = "1.0.0"
```

Every event and snapshot carries a `schema_version` field. Bumping the constant is a **breaking change** that must be coordinated:

1. Producer announces the upcoming version.
2. Silver adds a code path for the new version.
3. Producer flips the constant.
4. Silver drops the old code path after the watermark for old data passes.

The constant lives in its own module (`src/common/version.py`) so pure-Python callers — the simulator, the config loader, tests — can import it without dragging in PySpark via `schemas.py`.

---

## 2 · Entity relationships

```
                ┌─────────────────┐
                │     users       │ 1
                └────────┬────────┘
                         │
                         │ N
                ┌────────▼────────┐                   ┌──────────────┐
                │     events      │ N ───────────── 1 │   sessions   │
                └────────┬────────┘   (re-derived)    │ (silver only)│
                         │                            └──────────────┘
                         │ N (purchase events)
                         │
                ┌────────▼────────┐
                │   fact_orders   │
                └────────┬────────┘
                         │ collect_list
                         │
                ┌────────▼────────┐
                │     items[]     │ N
                └────────┬────────┘
                         │ N
                         │
                ┌────────▼────────┐
                │    products     │ 1
                └─────────────────┘
```

- `users` → `events`: a user emits many events; events may be anonymous (`user_id IS NULL`).
- `events` → `session`: many events compose one session; **session is derived in Silver**, not at the source.
- `events` (purchases only) → `fact_orders`: many purchase line-item events collapse into one order row.
- `fact_orders.items[]` → `products`: each line item references a product (joined via SCD2 with `order_ts BETWEEN valid_from AND valid_to`).

---

## 3 · Source schemas (Bronze contract)

These are the producer-facing schemas. Every byte that lands in the Volume must conform — anything else goes to `_rescued_data`.

### 3.1 Event envelope (`EVENT_SCHEMA`)

```python
EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),    False),
    StructField("event_type",     StringType(),    False),
    StructField("event_ts",       TimestampType(), False),
    StructField("user_id",        StringType(),    True),   # null = anonymous
    StructField("session_id",     StringType(),    False),
    StructField("device",         StringType(),    True),
    StructField("user_agent",     StringType(),    True),
    StructField("ip",             StringType(),    True),
    StructField("country",        StringType(),    True),
    StructField("page_url",       StringType(),    True),
    StructField("referrer",       StringType(),    True),
    StructField("product_id",     StringType(),    True),
    StructField("category",       StringType(),    True),
    StructField("price",          DoubleType(),    True),
    StructField("quantity",       IntegerType(),   True),
    StructField("cart_id",        StringType(),    True),
    StructField("order_id",       StringType(),    True),
    StructField("payment_method", StringType(),    True),
    StructField("discount_code",  StringType(),    True),
    StructField("properties",     MapType(StringType(), StringType()), True),
    StructField("schema_version", StringType(),    False),
])
```

#### Why a single envelope schema (not four)

There are four event types — `page_view`, `add_to_cart`, `purchase`, `abandon_cart` — and a temptation to split them into four schemas. We chose one envelope with **`event_type` as a discriminator** because:

- **Stable storage shape.** Bronze stores all four in one Delta table (`events_raw`); a single envelope means one schema, one table, one Auto Loader stream.
- **Simpler joins.** Funnel analysis crosses event types in a single query without a `UNION ALL`.
- **Optional fields are explicit.** `quantity` is null on `page_view` events; `order_id` is null on everything but `purchase`. The `event_type` discriminator tells consumers which fields are populated.

#### NOT NULL fields

`event_id`, `event_type`, `event_ts`, `session_id`, `schema_version` are non-nullable. These are **invariants every row must satisfy**, regardless of event type. Optional fields are nullable; the discriminator drives which nullable fields are populated for which type.

### 3.2 User snapshot schema (`USER_SCHEMA`)

```python
USER_SCHEMA = StructType([
    StructField("user_id",          StringType(),    False),
    StructField("email",            StringType(),    True),
    StructField("first_name",       StringType(),    True),
    StructField("last_name",        StringType(),    True),
    StructField("country",          StringType(),    True),
    StructField("city",             StringType(),    True),
    StructField("signup_ts",        TimestampType(), False),
    StructField("marketing_opt_in", BooleanType(),   True),
    StructField("loyalty_tier",     StringType(),    True),
    StructField("updated_ts",       TimestampType(), False),
    StructField("schema_version",   StringType(),    False),
])
```

#### Why snapshots, not CDC

A real producer would emit CDC events (`UPDATE old → new`). A simulator that emits CDC complicates the simulator without educational benefit. The snapshot model — *"here is the user's state as of `updated_ts`"* — is sufficient because Silver's SCD2 builder constructs the change history from snapshots via attribute hashing.

In production, CDC changes the Bronze contract slightly (a `before` and `after` payload, or a Debezium envelope), but **Silver's SCD2 logic is unchanged**: it still hashes tracked attributes, still detects boundaries.

### 3.3 Product snapshot schema (`PRODUCT_SCHEMA`)

```python
PRODUCT_SCHEMA = StructType([
    StructField("product_id",     StringType(),    False),
    StructField("sku",            StringType(),    False),
    StructField("name",           StringType(),    False),
    StructField("category",       StringType(),    False),
    StructField("subcategory",    StringType(),    True),
    StructField("brand",          StringType(),    True),
    StructField("price",          DoubleType(),    False),
    StructField("currency",       StringType(),    False),
    StructField("active",         BooleanType(),   False),
    StructField("updated_ts",     TimestampType(), False),
    StructField("schema_version", StringType(),    False),
])
```

`product_id` is the natural key; `sku` is the human-readable identifier. Both are tracked but only `product_id` is the join key.

### 3.4 Order schema (`ORDER_SCHEMA`)

Defined for completeness — used as a contract reference. The actual order rows are derived in Silver from purchase events; this schema describes what they end up looking like:

```python
ORDER_SCHEMA = StructType([
    StructField("order_id",       StringType(),    False),
    StructField("user_id",        StringType(),    False),
    StructField("session_id",     StringType(),    True),
    StructField("order_ts",       TimestampType(), False),
    StructField("status",         StringType(),    False),
    StructField("currency",       StringType(),    False),
    StructField("subtotal",       DoubleType(),    False),
    StructField("tax",            DoubleType(),    False),
    StructField("shipping",       DoubleType(),    False),
    StructField("total",          DoubleType(),    False),
    StructField("payment_method", StringType(),    True),
    StructField("country",        StringType(),    True),
    StructField("items",          ArrayType(ORDER_ITEM_SCHEMA), False),
    StructField("schema_version", StringType(),    False),
])
```

---

## 4 · Bronze tables (typed + metadata)

Every Bronze table = source schema + three metadata columns:

| Column | Type | Source | Why |
|---|---|---|---|
| `_ingest_ts`   | `timestamp` | `current_timestamp()` | Dedup tiebreaker in Silver |
| `_source_file` | `string`    | `_metadata.file_path` | Trace any anomaly to its raw file |
| `_ingest_date` | `date`      | `to_date(_ingest_ts)` | Partition column on `events_raw` only |

`events_raw` is partitioned by `_ingest_date`; `users_raw` and `products_raw` are not (snapshot volume is too small for partitioning to pay off).

See [`bronze_layer.md`](./bronze_layer.md) for the full DDL and table properties.

---

## 5 · Silver tables (modelled, conformed)

### 5.1 `silver.events`

Bronze schema + `event_date` (date) + `session_id_silver` (string) + `session_id_raw` (renamed from `session_id`). The producer's `session_id` is preserved for debugging; the canonical key is `session_id_silver = <user_id|anon>_<seq>`. Filtered to non-null `event_id`/`event_ts` and the four valid `event_type` values.

### 5.2 `silver.dim_users_scd2`

User schema + four SCD2 columns:

| Column | Type | Semantics |
|---|---|---|
| `valid_from` | `timestamp` | Inclusive |
| `valid_to`   | `timestamp` | Exclusive (NULL = current) |
| `is_current` | `boolean`   | `valid_to IS NULL` |
| `version`    | `int`       | 1-based |

Tracked attributes (hashed for change detection): `email, country, city, marketing_opt_in, loyalty_tier`.

### 5.3 `silver.dim_products_scd2`

Product schema + the same four SCD2 columns. Tracked: `name, category, subcategory, brand, price, currency, active`.

#### Why `price` is tracked

A retailer must attribute past orders to the price *at the time of sale*, not today's price. Hence the SCD2 join in Gold:

```sql
SELECT *
FROM   silver.fact_orders o
JOIN   silver.dim_products_scd2 p
  ON   o.items[i].product_id = p.product_id
 AND   o.order_ts BETWEEN p.valid_from
                       AND COALESCE(p.valid_to, '9999-12-31');
```

### 5.4 `silver.fact_orders`

Order grain — one row per `order_id`. Schema matches `ORDER_SCHEMA` plus `order_date` (partition column).

`items` is `ARRAY<STRUCT<product_id, category, quantity, unit_price, line_total>>` — denormalised line items embedded in the order. Gold marts read this struct directly; nothing else has to know that an order had multiple lines.

#### Why `items` is a struct array, not a flat join table

- Order grain is the natural unit for analytics. Most queries answer "how many orders / total dollars / average?" — not "what are all line items across all orders?".
- A struct array preserves order/line co-location (Delta stores them in the same row, the same file).
- For the rare line-item query, `EXPLODE(items)` is a one-liner.

---

## 6 · Gold tables (consumer-shaped)

| Table | Grain | Key columns |
|---|---|---|
| `fact_daily_sales`     | day × category × country | `(sale_date, category, country)` |
| `fact_funnel`          | day                      | `event_date` |
| `fact_abandoned_carts` | session                  | `session_id_silver` |
| `dim_user_360`         | user                     | `user_id` (one row per user; current SCD2 only) |

Full column lists in [`gold_layer.md`](./gold_layer.md).

---

## 7 · Modelling decisions and tradeoffs

| Decision | Alternative | Why |
|---|---|---|
| Single event envelope with discriminator | One schema per event type | Stable storage shape; simpler joins; matches industry-standard event modelling (Segment, Mixpanel) |
| Snapshot user/product source | CDC source | Simpler simulator; SCD2 logic in Silver is identical either way |
| `_ingest_date` partition only on events | Partition all Bronze tables | Snapshot volume is too small for partitioning to pay off |
| `items` as struct array on `fact_orders` | Separate `fact_order_items` table | Order grain is the natural analytics unit; co-location wins |
| SCD2 on users *and* products | Track only users | Price-at-sale-time is a hard retail requirement |
| `SCHEMA_VERSION` in code | Schema registry | Sufficient at this scale; trivially migrates to Confluent / Apicurio later |
| `properties: MAP<STRING,STRING>` on events | Pinned per-event-type schema | Lets the producer attach campaign / experiment metadata without bumping `SCHEMA_VERSION` for additive changes |
| Anonymous events (`user_id IS NULL`) preserved | Drop them | They count toward sessions, GMV, and funnel — losing them under-reports the business |

---

## 8 · Examples

### One row from the Bronze envelope

```json
{
  "event_id": "e_a8f9c1...",
  "event_type": "add_to_cart",
  "event_ts": "2026-04-27T10:35:12Z",
  "user_id": "u_42",
  "session_id": "s_kafka_77",
  "device": "mobile",
  "country": "BR",
  "product_id": "p_7d11",
  "category": "electronics",
  "price": 199.00,
  "quantity": 1,
  "cart_id": "c_42_1",
  "properties": {"campaign": "mobile_push_q2"},
  "schema_version": "1.0.0"
}
```

### One row from `silver.events` (same event, after Silver)

Adds `session_id_silver = "u_42_3"`, `event_date = 2026-04-27`, renames `session_id` → `session_id_raw`, fills `_ingest_ts`/`_source_file` from Bronze metadata.

### One row from `fact_orders`

```json
{
  "order_id": "o_777",
  "user_id": "u_42",
  "session_id_silver": "u_42_3",
  "country": "BR",
  "payment_method": "credit_card",
  "order_ts": "2026-04-27T10:42:11Z",
  "items": [
    {"product_id": "p_aaa", "category": "electronics", "quantity": 1, "unit_price": 199.0, "line_total": 199.0},
    {"product_id": "p_bbb", "category": "electronics", "quantity": 2, "unit_price": 79.0,  "line_total": 158.0}
  ],
  "subtotal": 357.0,
  "tax": 28.56,
  "shipping": 0.0,
  "total": 385.56,
  "currency": "USD",
  "status": "confirmed",
  "order_date": "2026-04-27"
}
```
