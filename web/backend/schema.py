"""Pydantic event model — mirror of ``src/common/schemas.py::EVENT_SCHEMA``.

Field order, names, types, and nullability match the Spark StructType
exactly so the NDJSON written to the landing volume is parsed by Auto
Loader without rows landing in ``_rescued_data``.

Constants like ``SCHEMA_VERSION`` and ``VALID_EVENT_TYPES`` are
duplicated here on purpose: this is a separate process that talks to
Databricks over HTTP and cannot import from ``src/`` (which depends on
PySpark). The duplication is deliberate, not accidental — keep it in
lockstep when bumping the producer contract.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Lockstep mirror of ``src/common/version.py``. Bump when the producer/
# consumer contract changes.
SCHEMA_VERSION = "1.0.0"

VALID_EVENT_TYPES = ("page_view", "add_to_cart", "purchase", "abandon_cart")
EventType = Literal["page_view", "add_to_cart", "purchase", "abandon_cart"]


class Event(BaseModel):
    """One row matching ``EVENT_SCHEMA``.

    Field order matches the Spark schema. Optional fields default to
    ``None`` and are emitted as JSON ``null`` so the type pinning in
    Auto Loader keeps them as the declared type rather than coercing
    them away.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    event_id: str = Field(default_factory=lambda: f"e_{uuid.uuid4().hex}")
    event_type: EventType
    event_ts: datetime
    user_id: str | None = None
    session_id: str
    device: str | None = None
    user_agent: str | None = None
    ip: str | None = None
    country: str | None = None
    page_url: str | None = None
    referrer: str | None = None
    product_id: str | None = None
    category: str | None = None
    price: float | None = None
    quantity: int | None = None
    cart_id: str | None = None
    order_id: str | None = None
    payment_method: str | None = None
    discount_code: str | None = None
    properties: dict[str, str] | None = None
    schema_version: str = SCHEMA_VERSION

    @field_validator("event_ts")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        """Normalise to UTC — Auto Loader expects ISO-8601 timestamps."""
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("properties")
    @classmethod
    def _stringify_props(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        """``MapType(StringType, StringType)`` — coerce values to str."""
        if value is None:
            return None
        return {str(k): str(v) for k, v in value.items()}

    def to_wire(self) -> dict[str, object]:
        """Serialise to the JSON shape Spark expects.

        Pydantic's default JSON encoder for ``datetime`` produces
        ISO-8601, which Spark's TimestampType parser accepts natively.
        """
        data = self.model_dump(mode="json")
        # Pydantic emits ``...+00:00``; Spark prefers a trailing 'Z'.
        ts = data.get("event_ts")
        if isinstance(ts, str) and ts.endswith("+00:00"):
            data["event_ts"] = ts.replace("+00:00", "Z")
        return data


class EventBatch(BaseModel):
    """Wire shape for ``POST /events/batch``."""

    model_config = ConfigDict(extra="forbid")

    events: list[Event] = Field(min_length=1, max_length=1000)


class SimulateRequest(BaseModel):
    """Wire shape for ``POST /simulate``."""

    model_config = ConfigDict(extra="forbid")

    n_sessions: int = Field(default=10, ge=1, le=500)
    seed: int | None = None
    country: str = "US"
