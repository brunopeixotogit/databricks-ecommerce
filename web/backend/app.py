"""FastAPI app — receives events from the frontend / simulator and
forwards them to a Databricks Volume as NDJSON.

Endpoints:
    POST /event         — single event
    POST /events/batch  — list of events (preferred; one file per call)
    POST /simulate      — server-side synthetic sessions
    GET  /health        — liveness + config probe
    GET  /              — serves the frontend (static)

The app reads configuration from environment variables only (see
``web/config/.env.example``). Frontend assets are served from
``web/frontend/`` so a single ``uvicorn`` process can run the whole
demo on Free Edition without a separate reverse proxy.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .databricks_client import (
    DatabricksConfigError,
    DatabricksUploadError,
    DatabricksVolumeClient,
)
from .schema import Event, EventBatch, SimulateRequest
from .simulator import simulate_batch

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("bricksshop.web")

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        client = DatabricksVolumeClient()
    except DatabricksConfigError as exc:
        logger.error("Startup failed: %s", exc)
        raise
    app.state.dbx = client
    logger.info(
        "Databricks client ready: host=%s volume=%s dry_run=%s",
        client.host, client.volume_path, client.dry_run,
    )
    try:
        yield
    finally:
        client.close()


app = FastAPI(
    title="BricksShop API",
    description="BricksShop e-commerce simulation feeding events into Databricks.",
    version="0.1.0",
    lifespan=lifespan,
)

# Permissive CORS for local dev. Tighten the origin list for any
# real-world deployment.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _client(req: Request) -> DatabricksVolumeClient:
    return req.app.state.dbx


def _enrich_from_request(event: Event, request: Request) -> Event:
    """Fill in transport-level fields the browser cannot reliably set."""
    if event.ip is None:
        client = request.client
        event.ip = client.host if client else None
    if event.user_agent is None:
        event.user_agent = request.headers.get("user-agent")
    return event


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "databricks": _client(request).health(),
    }


@app.post("/event")
async def post_event(event: Event, request: Request) -> JSONResponse:
    """Accept one event. Writes a single-line NDJSON file to the volume."""
    event = _enrich_from_request(event, request)
    try:
        result = _client(request).upload_events([event.to_wire()])
    except DatabricksUploadError as exc:
        logger.exception("Upload failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return JSONResponse(content=result)


@app.post("/events/batch")
async def post_events_batch(batch: EventBatch, request: Request) -> JSONResponse:
    """Accept a list of events. Writes one NDJSON file."""
    enriched = [_enrich_from_request(e, request).to_wire() for e in batch.events]
    try:
        result = _client(request).upload_events(enriched)
    except DatabricksUploadError as exc:
        logger.exception("Batch upload failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return JSONResponse(content=result)


@app.post("/simulate")
async def post_simulate(req: SimulateRequest, request: Request) -> JSONResponse:
    """Generate ``n_sessions`` synthetic sessions and upload them."""
    events = simulate_batch(req.n_sessions, seed=req.seed, country=req.country)
    if not events:
        return JSONResponse(content={"status": "noop", "events": 0})
    try:
        result = _client(request).upload_events([e.to_wire() for e in events])
    except DatabricksUploadError as exc:
        logger.exception("Simulate upload failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    result["sessions"] = req.n_sessions
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# Static frontend — mounted last so API routes win on path conflicts.
# ---------------------------------------------------------------------------
if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    logger.warning("Frontend dir not found at %s — static mount skipped.", FRONTEND_DIR)
