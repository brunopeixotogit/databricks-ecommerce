"""Public API for synthetic event generation — fully decoupled from the
processing pipeline.

This module is the single entry point the orchestrator uses to produce
test data. It does not import from ``src.bronze``, ``src.silver``, or
``src.gold``; it does not call DLT; it does not touch any Delta table.
It only writes NDJSON files to the events landing folder, which the
processing layer is free to ingest on its own schedule.

Design contract:
    * One call writes one NDJSON file containing exactly ``n_events``
      events. ``event_id`` values are fresh UUID4s, so re-runs never
      collide and Silver's ``MERGE`` on ``event_id`` stays idempotent.
    * Every event carries ``properties["source"] = source`` (default
      ``"simulator"``) so downstream consumers can filter synthetic data
      out of analytics if they choose.
    * The schema is a strict subset of ``EVENT_SCHEMA`` — adding or
      removing a field would break Bronze's pinned StructType, so the
      function never touches the field set.
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Mapping
from datetime import date as date_cls
from datetime import datetime, timezone
from pathlib import Path

from src.common.config import load_config
from src.simulator.run import (
    bootstrap_entities,
    make_session_stream,
)

DEFAULT_SOURCE = "simulator"


def _resolve_date(date: str | date_cls | None) -> str:
    if date is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if isinstance(date, date_cls):
        return date.strftime("%Y-%m-%d")
    # Accept and validate "YYYY-MM-DD"
    datetime.strptime(date, "%Y-%m-%d")
    return date


def _stream_n_events(stream_factory, target: int, *, batch: int = 200) -> Iterator[dict]:
    """Drain the underlying session stream until ``target`` events are
    produced. The session FSM emits a variable number of events per
    session, so we ask for sessions in batches and stop once the count
    is reached.
    """
    emitted = 0
    while emitted < target:
        for ev in stream_factory(batch):
            yield ev
            emitted += 1
            if emitted >= target:
                return


def _decorate(event: dict, *, source: str, run_id: str) -> dict:
    """Inject producer-side metadata via the existing ``properties``
    map column. No top-level fields are added — the StructType pinned
    in ``src/common/schemas.py`` stays intact."""
    props = dict(event.get("properties") or {})
    props["source"] = source
    props["run_id"] = run_id
    event["properties"] = props
    return event


def _serialize_ndjson(rows: list[dict]) -> bytes:
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat().replace("+00:00", "Z")
        raise TypeError(f"not serializable: {type(o).__name__}")

    return ("\n".join(json.dumps(r, default=default, separators=(",", ":")) for r in rows)
            + "\n").encode("utf-8")


def generate_events(
    n_events: int,
    date: str | date_cls | None = None,
    *,
    landing_root: str,
    source: str = DEFAULT_SOURCE,
    cfg: Mapping | None = None,
) -> dict:
    """Emit exactly ``n_events`` synthetic events as one NDJSON file.

    Parameters
    ----------
    n_events:
        Number of events to write. Must be > 0.
    date:
        Partition date for the output file. ``"YYYY-MM-DD"`` string,
        ``datetime.date``, or ``None`` (today in UTC).
    landing_root:
        Absolute landing volume root, e.g.
        ``/Volumes/dev_main/ecom_bronze/landing``. The function writes
        under ``{landing_root}/events/dt={date}/``.
    source:
        Tag written to ``properties["source"]`` on every event so
        consumers can distinguish synthetic data. Default ``"simulator"``.
    cfg:
        Optional simulator config (a dict shaped like ``conf/simulator.yml``).
        When ``None``, falls back to ``load_config("simulator")``.

    Returns
    -------
    A small summary dict suitable for logging / notebook display::

        {
            "events_written": int,
            "path":          str,    # absolute path of the file written
            "partition":     str,    # "dt=YYYY-MM-DD"
            "source":        str,
            "run_id":        str,    # uniquely identifies this batch
        }
    """
    if n_events <= 0:
        raise ValueError(f"n_events must be > 0, got {n_events}")

    cfg = cfg or load_config("simulator")
    partition = _resolve_date(date)
    run_id = uuid.uuid4().hex

    # Build the deterministic event stream from the existing FSM.
    users, products = bootstrap_entities(cfg)
    stream_factory = make_session_stream(users, products, cfg)

    rows = [
        _decorate(ev, source=source, run_id=run_id)
        for ev in _stream_n_events(stream_factory, n_events)
    ]

    out_dir = Path(landing_root) / "events" / f"dt={partition}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"events_{run_id}.ndjson"
    out_path.write_bytes(_serialize_ndjson(rows))

    return {
        "events_written": len(rows),
        "path": str(out_path),
        "partition": f"dt={partition}",
        "source": source,
        "run_id": run_id,
    }
