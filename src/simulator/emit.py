"""Write simulated events / snapshots to the landing volume.

Events are written as gzipped, line-delimited JSON. Files are
partitioned by ingest date (``dt=YYYY-MM-DD``) so Auto Loader scans
remain bounded as the volume grows.

Auto Loader picks these up via ``cloudFiles`` with the schema pinned in
``src.common.schemas`` — there is intentionally no schema inference at
the producer / consumer boundary.
"""
from __future__ import annotations

import gzip
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List


def _default_serializer(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"Type not serializable: {type(o).__name__}")


def _serialize(rows: List[dict]) -> bytes:
    return ("\n".join(json.dumps(r, default=_default_serializer) for r in rows)).encode("utf-8")


def emit_events(
    events: Iterable[dict],
    landing_dir: str,
    rows_per_file: int = 5000,
    compress: bool = True,
) -> Iterator[str]:
    """Stream ``events`` to ``landing_dir`` in batches. Yields written paths."""
    Path(landing_dir).mkdir(parents=True, exist_ok=True)
    batch: List[dict] = []
    for ev in events:
        batch.append(ev)
        if len(batch) >= rows_per_file:
            yield _flush(batch, landing_dir, compress)
            batch = []
    if batch:
        yield _flush(batch, landing_dir, compress)


def emit_snapshot(rows: Iterable[dict], landing_dir: str, name: str) -> str:
    """Write a single full snapshot file (users / products / inventory)."""
    Path(landing_dir).mkdir(parents=True, exist_ok=True)
    path = os.path.join(landing_dir, f"{name}-{uuid.uuid4().hex}.json")
    payload = _serialize(list(rows))
    with open(path, "wb") as f:
        f.write(payload)
    return path


def _flush(batch: List[dict], landing_dir: str, compress: bool) -> str:
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    part_dir = os.path.join(landing_dir, f"dt={dt}")
    Path(part_dir).mkdir(parents=True, exist_ok=True)
    fname = f"events-{uuid.uuid4().hex}.json"
    if compress:
        fname += ".gz"
    path = os.path.join(part_dir, fname)
    payload = _serialize(batch)
    if compress:
        with gzip.open(path, "wb") as f:
            f.write(payload)
    else:
        with open(path, "wb") as f:
            f.write(payload)
    return path
