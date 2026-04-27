# Bronze Layer

The Bronze layer is the **replay log** for the lakehouse. It captures everything the producer emits, in the form it emits it, and never mutates it afterward. Every other layer can be rebuilt from Bronze; Bronze cannot be rebuilt from anywhere else.

Code: `src/bronze/ingest_events.py`, `notebooks/20_bronze.py`, `notebooks/01_create_tables.py`.

---

## 1 · Ingestion strategy — Auto Loader (`cloudFiles`)

Three independent streams ingest from the landing Volume:

| Source | Folder | Bronze table | Cadence |
|---|---|---|---|
| events   | `landing/events/dt=YYYY-MM-DD/` | `bronze.events_raw`   | continuous |
| users    | `landing/users/`                | `bronze.users_raw`    | snapshots |
| products | `landing/products/`             | `bronze.products_raw` | snapshots |

```python
raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
       .option("cloudFiles.inferColumnTypes", "false")
       .option("cloudFiles.schemaEvolutionMode", "rescue")
       .option("cloudFiles.includeExistingFiles", "true")
       .schema(schema)
       .load(landing_path))
```

### Why Auto Loader (and not `spark.read.json` in batch)?

- **Incremental discovery.** Auto Loader maintains a checkpoint of files already processed. Re-running the notebook does **not** re-ingest old files — even when they're still in the Volume.
- **Scales to billions of files.** RocksDB-backed file notification mode handles the file-listing cost that breaks naïve `ls`-based ingestion.
- **Native rescue mode.** Unknown / mistyped fields land in `_rescued_data` instead of crashing the job.
- **Stream / batch unified API.** `trigger=availableNow` makes a stream behave exactly like a batch, so we don't pay for a second code path.

### Trigger choice — `availableNow` by default

```python
writer = writer.trigger(availableNow=True)        # default
# or
writer = writer.trigger(processingTime="30 seconds")  # production tier
```

**Reason:** on Databricks Free Edition, holding a stream alive 24/7 wastes the shared cluster. `availableNow` processes any new files since the last run and exits, which is ideal for cron-scheduled Workflows. A single config flip (`conf/pipeline.yml → streaming.trigger`) switches to continuous in a paid environment without code changes.

---

## 2 · Schema enforcement

Schemas are pinned in `src/common/schemas.py` (`EVENT_SCHEMA`, `USER_SCHEMA`, `PRODUCT_SCHEMA`). Auto Loader is configured with:

```python
.option("cloudFiles.inferColumnTypes",    "false")
.option("cloudFiles.schemaEvolutionMode", "rescue")
.schema(EVENT_SCHEMA)
```

### How drift is handled

| Producer change | What happens |
|---|---|
| Field's type changes (`price: double` → `string`) | Mismatched value lands in `_rescued_data`; the typed `price` stays null |
| New unknown field appears | Lands in `_rescued_data` with original key/value |
| Required field missing | Row stays in the table; downstream (Silver) filters drop it via `IS NOT NULL` checks |
| `schema_version` bumped | Producer-coordinated breaking change — see § 6 below |

### Why this is better than `inferSchema`

`inferSchema` reads a sample of files to guess types. The first time a string sneaks into a numeric column, the inferred schema flips and **every** downstream join silently breaks. Pinning makes drift loud — a non-empty `_rescued_data` is a measurable, alertable signal.

---

## 3 · Append-only design

Bronze tables are created with the Delta property:

```sql
TBLPROPERTIES ('delta.appendOnly' = 'true')
```

This **forbids** `UPDATE` and `DELETE` against the table at the engine level. Combined with Auto Loader's append-only `outputMode`, Bronze becomes a true replay log.

### Why this matters

- **Reproducibility.** Re-running Silver from any historical Bronze snapshot produces the same Silver. There is no "what did Bronze look like yesterday?" question — the answer is `DESCRIBE HISTORY` or `VERSION AS OF`.
- **Forensic debugging.** When a Gold KPI shifts unexpectedly, you can pin the exact Bronze rows that contributed and replay Silver against them.
- **Cheap GDPR posture.** Deletions for compliance reasons are surgical: delete the Bronze records, then rebuild Silver and Gold. The audit trail is the Delta history.

### Tradeoff

You cannot fix a bad Bronze row in place. You add a corrective row (in a real producer scenario, a CDC event), and Silver's dedup logic picks the latest. This is a feature, not a bug — it preserves auditability.

---

## 4 · Bronze metadata columns

Every row gets three columns appended at ingest time:

| Column | Type | Source |
|---|---|---|
| `_ingest_ts`   | `timestamp` | `F.current_timestamp()` at write time |
| `_source_file` | `string`    | `F.col("_metadata.file_path")` |
| `_ingest_date` | `date`      | `F.to_date("_ingest_ts")` — events only |

### Why these specifically

- `_ingest_ts` is the dedup tiebreaker in Silver (`row_number() OVER (PARTITION BY event_id ORDER BY _ingest_ts DESC)`).
- `_source_file` lets you trace any anomaly back to its raw file in the Volume.
- `_ingest_date` is the **partition column** for `events_raw`. Bounded scan range = bounded join cost downstream.

---

## 5 · Dedup at the ingestion level

Bronze does **not** dedup. Auto Loader's checkpoint guarantees each *file* is read once, but the same logical event may legitimately arrive multiple times (producer retry, CDC update, batch replay).

### Why dedup belongs in Silver, not Bronze

- Bronze is the replay log. Discarding "duplicate" rows there destroys evidence we may need for debugging or compliance.
- The natural dedup key (`event_id`) plus the tiebreaker (`_ingest_ts`) live across the two layers; doing the dedup once in Silver is simpler than fragmenting it.
- Auto Loader's exactly-once guarantee is at the **file** level, not the row level. Trying to also dedup rows inside Bronze duplicates concerns and doesn't actually buy us anything.

So Bronze gets at-least-once row delivery; Silver enforces exactly-once on `event_id`. See [`silver_layer.md`](./silver_layer.md) for the dedup window expression.

---

## 6 · Schema versioning contract

`SCHEMA_VERSION = "1.0.0"` lives in `src/common/version.py` and is stamped on every event by the producer (`schema_version` field). Bumping the version is a coordinated breaking change:

1. Producer team announces the upcoming version.
2. Consumers (Silver) add a code path for the new version.
3. Producer flips the constant; new files carry the new version.
4. Consumer drops the old code path after the watermark for old data passes.

This is the same protocol you'd use against a Confluent Schema Registry — we just keep the source of truth in code instead of a registry, which is fine for this scale.

---

## 7 · Table creation — explicit DDL (`notebooks/01_create_tables.py`)

Bronze tables are pre-created with explicit DDL, not auto-created on first write:

```sql
CREATE TABLE IF NOT EXISTS main.ecom_bronze.events_raw (
    event_id STRING NOT NULL,
    -- ... full pinned schema ...
    _ingest_ts TIMESTAMP NOT NULL,
    _source_file STRING,
    _ingest_date DATE NOT NULL
)
USING DELTA
PARTITIONED BY (_ingest_date)
TBLPROPERTIES (
    'delta.appendOnly'                  = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'pipeline.layer'                    = 'bronze',
    'pipeline.schema_version'           = '1.0.0'
)
```

### Why pre-create

- **Schema is locked at row 0.** Auto Loader's first micro-batch can't accidentally widen a column.
- **Partitioning is locked at creation.** Adding partitioning later to a non-partitioned table requires a rewrite.
- **Properties are explicit.** `appendOnly`, column mapping, auto-optimize — all set up front, not retro-fitted.
- **Discoverability.** `pipeline.layer` and `pipeline.schema_version` are queryable via `information_schema`, so a dashboard can show "what version of the contract is each table on?"

---

## 8 · Sanity checks

`notebooks/20_bronze.py` ends with a row-count smoke query against the three Bronze tables. The failure mode it catches: an Auto Loader stream silently never made progress because the producer wrote files to a path the stream isn't watching.

```sql
SELECT 'events_raw' AS table, COUNT(*) FROM main.ecom_bronze.events_raw
UNION ALL SELECT 'users_raw',    COUNT(*) FROM main.ecom_bronze.users_raw
UNION ALL SELECT 'products_raw', COUNT(*) FROM main.ecom_bronze.products_raw
```

Real validation lives in `99_quality_checks` against Silver — Bronze itself is intentionally permissive. See [`data_quality.md`](./data_quality.md).

---

## 9 · Common Bronze failure modes

| Symptom | Likely cause | Remedy |
|---|---|---|
| Stream runs but row count stays at 0 | Producer writing to wrong path; checkpoint pointing at wrong source | Verify `landing_path` widget vs. simulator output dir; inspect `_checkpoints/<source>/sources/` |
| `_rescued_data` non-empty | Producer drift | Inspect rescued payloads, decide whether to bump `SCHEMA_VERSION` |
| Auto Loader re-ingests every file | Checkpoint deleted or moved | Treat checkpoints as durable infrastructure; if forced, drop target table **and** checkpoint together |
| `DELTA_TABLE_NOT_FOUND` on first run | Bronze tables not pre-created | Run `notebooks/01_create_tables.py` |

See [`runbook.md`](./runbook.md) for the full operational playbook.
