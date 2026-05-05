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

from .agents import Agents, ConversationStore
from .chat_schema import (
    AddToCartRequest,
    AddToCartResponse,
    ChatProduct,
    ChatRequest,
    ChatResponse,
)
from .databricks_client import (
    DatabricksConfigError,
    DatabricksUploadError,
    DatabricksVolumeClient,
)
from .faiss_index import FaissProductIndex, FaissUnavailable
from .llm_client import DatabricksLLM, LLMConfigError
from .products import ProductCatalog, ProductLookupError
from .ranking import HybridRanker, PopularitySignals, PopularityUnavailable
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

    # Optional chat stack — failure here must not block event ingestion,
    # which is the existing app's primary job.
    app.state.llm = None
    app.state.catalog = None
    app.state.agents = None
    app.state.faiss = None
    app.state.popularity = None
    app.state.ranker = None
    if not client.dry_run:
        try:
            app.state.llm = DatabricksLLM()
            app.state.catalog = ProductCatalog()
            app.state.agents = Agents(app.state.llm, app.state.catalog, ConversationStore())
            logger.info(
                "Chat stack ready: router=%s composer=%s table=%s",
                app.state.llm.router_endpoint,
                app.state.llm.chat_endpoint,
                app.state.catalog.table,
            )
        except (LLMConfigError, ProductLookupError) as exc:
            logger.warning("Chat stack disabled: %s", exc)

        # FAISS tier — best-effort. The pointer file may not exist yet
        # (no rebuild has run), or the embeddings model may be missing
        # locally; in either case we just skip this tier.
        if app.state.catalog is not None:
            try:
                fidx = FaissProductIndex()
                fidx.start()
                if fidx.is_ready():
                    app.state.catalog.attach_faiss(fidx)
                    app.state.faiss = fidx
                    logger.info("FAISS tier active: %s", fidx.info())
                else:
                    # Keep the poller running — it will pick up the index
                    # the moment a build job finishes and updates the pointer.
                    app.state.faiss = fidx
                    app.state.catalog.attach_faiss(fidx)
                    logger.info(
                        "FAISS tier idle (pointer not ready); poller will retry."
                    )
            except FaissUnavailable as exc:
                logger.warning("FAISS tier disabled: %s", exc)

        # Hybrid ranking — best-effort. The popularity cache may be
        # empty initially; the ranker tolerates that and emits 0.0 for
        # the popularity component until the poller fills it in.
        if app.state.catalog is not None:
            try:
                pop = PopularitySignals()
                pop.start()
                ranker = HybridRanker(popularity=pop)
                app.state.catalog.attach_ranker(ranker)
                app.state.popularity = pop
                app.state.ranker = ranker
                logger.info(
                    "Hybrid ranking active: weights=%s popularity=%s",
                    ranker.weights(), pop.info(),
                )
            except PopularityUnavailable as exc:
                logger.warning("Hybrid ranking disabled: %s", exc)
    try:
        yield
    finally:
        client.close()
        if app.state.llm is not None:
            app.state.llm.close()
        if app.state.faiss is not None:
            app.state.faiss.close()
        if app.state.popularity is not None:
            app.state.popularity.close()


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
# Chat endpoints (multi-agent, Databricks-native)
# ---------------------------------------------------------------------------
def _agents(req: Request) -> Agents:
    agents = req.app.state.agents
    if agents is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Chat stack not configured. Set DATABRICKS_TOKEN, "
                "SQL_WAREHOUSE_ID, and ensure the model serving endpoints are reachable."
            ),
        )
    return agents


def _catalog(req: Request) -> ProductCatalog:
    catalog = req.app.state.catalog
    if catalog is None:
        raise HTTPException(status_code=503, detail="Product catalog not configured.")
    return catalog


@app.post("/chat", response_model=ChatResponse)
async def post_chat(req: ChatRequest, request: Request) -> ChatResponse:
    agents = _agents(request)
    try:
        result = agents.handle(req.session_id, req.message)
    except Exception as exc:
        logger.exception("Chat orchestration failed")
        raise HTTPException(status_code=502, detail=f"Chat error: {exc}") from exc
    return ChatResponse(
        intent=result.intent,
        reply=result.reply,
        products=[ChatProduct(**p.to_dict()) for p in result.products],
        query=result.query,
        max_price=result.max_price,
    )


@app.post("/chat/add-to-cart", response_model=AddToCartResponse)
async def post_chat_add_to_cart(
    req: AddToCartRequest, request: Request
) -> AddToCartResponse:
    catalog = _catalog(request)
    try:
        product = catalog.get(req.product_id)
    except ProductLookupError as exc:
        raise HTTPException(status_code=502, detail=f"Catalog error: {exc}") from exc
    if product is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown product_id: {req.product_id}"
        )

    # Emit a real add_to_cart event into the landing volume so the
    # existing pipeline picks it up just like UI clicks do.
    event = Event(
        event_type="add_to_cart",
        event_ts=datetime.now(timezone.utc),
        session_id=req.session_id,
        user_id=req.user_id,
        product_id=product.product_id,
        category=product.category,
        price=product.price,
        quantity=req.quantity,
        cart_id=req.cart_id,
        properties={"source": "web_chat", "name": product.name},
    )
    event = _enrich_from_request(event, request)
    try:
        upload = _client(request).upload_events([event.to_wire()])
    except DatabricksUploadError as exc:
        logger.exception("Chat add-to-cart upload failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return AddToCartResponse(
        status="ok",
        product=ChatProduct(**product.to_dict()),
        quantity=req.quantity,
        event_path=str(upload.get("path", "")),
    )


# ---------------------------------------------------------------------------
# Static frontend — mounted last so API routes win on path conflicts.
# ---------------------------------------------------------------------------
if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    logger.warning("Frontend dir not found at %s — static mount skipped.", FRONTEND_DIR)
