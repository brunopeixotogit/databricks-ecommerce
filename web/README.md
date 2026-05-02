# `web/` — E-commerce simulation feeding the lakehouse

A small full-stack app that turns user clicks (and synthetic sessions) into events compatible with `src/common/schemas.py::EVENT_SCHEMA`, uploads them as NDJSON to a Databricks Volume, and lets the existing **Bronze → Silver → Gold** pipeline ingest them — through either the Workflow `medallion` job or the DLT pipeline.

```
Browser / simulator  →  FastAPI (this app)  →  Databricks Volumes Files API
                                              /Volumes/<cat>/<bronze_schema>/landing/events/
                                                 ↓ Auto Loader
                                              Bronze → Silver → Gold
```

> The pipeline itself is not modified — the existing `notebooks/20_bronze.py` and `pipelines/dlt/bronze.py` already watch this folder. This app is a new producer; everything downstream stays as-is.

---

## Folder layout

```
web/
├── frontend/                  # static HTML + Bootstrap + vanilla JS
│   ├── index.html             # product listing
│   ├── product.html           # product detail
│   ├── cart.html              # cart view
│   ├── checkout.html          # checkout + thank-you
│   ├── css/styles.css         # custom palette (teal / cream — not Amazon, not Magalu)
│   └── js/
│       ├── tracker.js         # event tracking + batch flush
│       ├── catalog.js         # static product list (mirrors backend simulator)
│       └── app.js             # shared helpers (cart badge)
├── backend/
│   ├── app.py                 # FastAPI: /event, /events/batch, /simulate, /health
│   ├── databricks_client.py   # Volumes Files API uploader (NDJSON, dt=YYYY-MM-DD)
│   ├── schema.py              # Pydantic mirror of EVENT_SCHEMA
│   ├── simulator.py           # server-side session FSM
│   └── requirements.txt
├── config/
│   └── .env.example
└── README.md
```

The frontend is served by FastAPI itself (`StaticFiles` mount at `/`), so a single process runs the whole demo.

---

## Configuration

Copy `web/config/.env.example` to `web/config/.env` and fill in:

| Variable             | Purpose                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------|
| `DATABRICKS_HOST`    | Workspace base URL (e.g. `https://dbc-xxx.cloud.databricks.com`). No trailing slash.       |
| `DATABRICKS_TOKEN`   | OAuth M2M token or PAT scoped to the dev workspace, with `WRITE FILES` on the volume.     |
| `VOLUME_PATH`        | Absolute path under `/Volumes/...` where event files land. Must match what Auto Loader watches. |
| `DATABRICKS_DRYRUN`  | `true` writes to local `./dryrun_events/` instead of calling the API. Default `false`.    |
| `CORS_ORIGINS`       | (Optional) comma-separated allow list. Default `*` for local dev.                          |
| `LOG_LEVEL`          | (Optional) `INFO` / `DEBUG`. Default `INFO`.                                              |

For dev with the existing pipeline, `VOLUME_PATH` is typically:

```
/Volumes/dev_main/ecom_bronze/landing/events
```

---

## Running it

```bash
# 1) Create + activate a venv
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate

# 2) Install deps
pip install -r web/backend/requirements.txt

# 3) Configure
cp web/config/.env.example web/config/.env
# edit web/config/.env with your DATABRICKS_HOST + DATABRICKS_TOKEN + VOLUME_PATH

# 4) Run (serves API + frontend on :8000)
uvicorn web.backend.app:app --reload --env-file web/config/.env
```

Open http://localhost:8000/ → BricksShop store. Click around, add to cart, check out. Every action posts to `/events/batch`, which uploads NDJSON files to the volume.

> **Tip — start in dry-run.** Set `DATABRICKS_DRYRUN=true` first; events land in `./dryrun_events/dt=YYYY-MM-DD/*.ndjson` so you can verify the payload shape locally before pointing at a real workspace.

---

## API

### `POST /event`
Single event. Body matches `EVENT_SCHEMA` (Pydantic enforces it).

```bash
curl -X POST http://localhost:8000/event \
  -H 'content-type: application/json' \
  -d '{
    "event_type": "page_view",
    "event_ts":   "2026-04-30T14:00:00Z",
    "session_id": "s_demo_1",
    "user_id":    "u_demo",
    "page_url":   "/"
  }'
```

### `POST /events/batch`
Preferred path. One file per call, up to 1000 events.

```bash
curl -X POST http://localhost:8000/events/batch \
  -H 'content-type: application/json' \
  -d '{ "events": [ /* ... */ ] }'
```

### `POST /simulate`
Server-side synthetic users — useful for stress-testing the pipeline without a browser.

```bash
curl -X POST http://localhost:8000/simulate \
  -H 'content-type: application/json' \
  -d '{ "n_sessions": 25, "seed": 42, "country": "US" }'
```

Generates ~6–14 events per session via the FSM in `simulator.py` and uploads them as one NDJSON file.

### `GET /health`
Returns `{ status, ts, databricks: { host, volume_path, dry_run } }`.

---

## Event flow — what actually happens

1. Browser fires `Tracker.pageView()` on every `DOMContentLoaded`. Adding to cart → `Tracker.addToCart`. Placing an order → `Tracker.purchase` (one event per line). Closing the tab with a non-empty cart → `Tracker.abandonCart`.
2. Tracker buffers events in memory and flushes **every 4s** (or on a hard cap of 25 events, or on `pagehide` via `sendBeacon`) to `POST /events/batch`.
3. FastAPI validates against the Pydantic model (`schema.py`), enriches `ip` / `user_agent` from the HTTP request, and serialises to NDJSON.
4. The Databricks client (`databricks_client.py`) PUTs the bytes to `/api/2.0/fs/files{VOLUME_PATH}/dt=YYYY-MM-DD/events_<uuid>.ndjson?overwrite=true`.
5. Auto Loader (`notebooks/20_bronze.py` and/or `pipelines/dlt/bronze.py`) discovers the new files on its next trigger, parses them with the pinned `EVENT_SCHEMA`, and appends to `events_raw`.
6. Silver and Gold proceed as documented in [`docs/architecture.md`](../docs/architecture.md).

Because the producer schema and `SCHEMA_VERSION = "1.0.0"` are an exact mirror of `src/common/schemas.py`, no rows should land in `_rescued_data`. **If they do, a producer field has drifted — fix here first.**

---

## Identity model (frontend)

| Concept       | Where it lives                     | Reset condition                                        |
|---------------|-------------------------------------|--------------------------------------------------------|
| `user_id`     | `localStorage["ecom.user_id"]`     | Never (clearing storage = new user)                    |
| `session_id`  | `localStorage["ecom.session"]`     | Idle > 30 min between events → new session             |
| `cart_id`     | `localStorage["ecom.cart"]`        | Cleared on `purchase` or removed manually               |

Silver re-derives `session_id_silver` from event-time gaps anyway, so the producer's `session_id` is preserved as `session_id_raw` and used only for debugging.

---

## End-to-end test plan

> Cluster running, the bundle deployed (`databricks bundle deploy --target dev`).

1. **Smoke test in dry-run.** `DATABRICKS_DRYRUN=true` → click around the UI → `cat ./dryrun_events/dt=$(date -u +%F)/*.ndjson | head` and confirm the JSON matches `EVENT_SCHEMA` (21 fields, `event_ts` ends in `Z`).
2. **Live upload.** Set `DATABRICKS_DRYRUN=false` and a valid token. Run `curl -s http://localhost:8000/health` — `databricks.dry_run` should be `false`.
3. **Trigger one purchase from the UI.** Verify a 200 from `/events/batch` in the browser network tab.
4. **Inspect the volume from Databricks.**
   ```sql
   LIST '/Volumes/dev_main/ecom_bronze/landing/events/' RECURSIVE;
   ```
   The new `dt=YYYY-MM-DD/events_*.ndjson` file should appear within seconds.
5. **Run Bronze.**
   - Workflow path: `databricks bundle run --target dev medallion` (the `bronze` task picks up the file).
   - DLT path: it runs on the next CD push or manually via `databricks bundle run --target dev ecom_dlt_pipeline`.
6. **Verify Bronze rows.**
   ```sql
   SELECT event_type, COUNT(*)
   FROM   dev_main.ecom_bronze.events_raw
   WHERE  _ingest_date = current_date()
   GROUP  BY event_type;
   ```
   Counts should reflect what you clicked.
7. **Check `_rescued_data`.** Should be empty for the rows produced by this app:
   ```sql
   SELECT COUNT(*) FROM dev_main.ecom_bronze.events_raw
   WHERE  _ingest_date = current_date() AND _rescued_data IS NOT NULL;
   ```
   Anything > 0 means a payload field drifted from `EVENT_SCHEMA`.
8. **Run Silver + Gold + quality** as usual to confirm the data flows all the way through to `fact_daily_sales` (`dev_main.ecom_gold.fact_daily_sales` for the Workflow path, `dev_main.ecom_dlt.fact_daily_sales` for DLT).

---

## Stress test

```bash
# 200 sessions = roughly 1.4–2.8k events in one NDJSON file.
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/simulate \
    -H 'content-type: application/json' \
    -d '{ "n_sessions": 200 }'
  echo
done
```

Each call produces one file — five calls give Auto Loader five files to chew on, which exercises the multi-file path without overwhelming the listing.

---

## Troubleshooting

| Symptom                                                              | Likely cause / fix                                                                                |
|----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| Startup error: `Missing required environment variable: DATABRICKS_HOST` | `.env` not loaded — start uvicorn with `--env-file web/config/.env`, or export the vars manually. |
| `502 Files API rejected upload: 403 PERMISSION_DENIED`               | Token lacks `WRITE FILES` on the volume. Grant via UC: `GRANT WRITE VOLUME ON VOLUME ... TO ...`.  |
| `502 Files API rejected upload: 404 RESOURCE_DOES_NOT_EXIST`         | `VOLUME_PATH` typo, or the volume hasn't been created yet (`notebooks/00_setup.py`).               |
| Browser network tab shows `CORS error` on `/events/batch`            | Opened the HTML file directly via `file://` — open via `http://localhost:8000/` instead.          |
| Bronze sees the files but `_rescued_data` is non-empty               | Some payload field doesn't match `EVENT_SCHEMA`. Diff against `web/backend/schema.py`; fix here.   |
| Tracker double-fires `page_view`                                     | Page reloaded; tracker fires once per `DOMContentLoaded` by design. Not a bug.                     |
| `abandon_cart` never appears                                         | Cart was empty, or `pagehide` did not fire (some browsers suppress it on hard reload). Try closing the tab. |

---

## Why this is shaped this way

- **One file per request, not a long-lived stream.** Files API uploads are the simplest way to feed Auto Loader without a streaming connector. NDJSON keeps each row independently parseable.
- **`dt=YYYY-MM-DD` partitioning is cosmetic.** Auto Loader ignores subfolders for content discovery; the prefix exists so a human can `ls` the landing zone without paging through thousands of files.
- **Pydantic mirror of `EVENT_SCHEMA`, not import.** `src/` depends on PySpark; this app is a vanilla web service. Duplication is deliberate — keep `web/backend/schema.py` and `src/common/schemas.py` in lockstep when bumping `SCHEMA_VERSION`.
- **No real auth on the API.** This is a demo on the same trust boundary as the developer's laptop. Add an API key middleware before exposing it externally.
