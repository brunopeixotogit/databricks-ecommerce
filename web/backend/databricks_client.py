"""Databricks Volumes Files API client.

Uploads NDJSON event files into the landing volume so Auto Loader (the
existing Bronze stream in `notebooks/20_bronze.py` and the DLT pipeline
in `pipelines/dlt/bronze.py`) picks them up exactly like simulator
output.

Endpoint reference:
    PUT /api/2.0/fs/files{abs_path}?overwrite=true
    Authorization: Bearer <token>

Files are written under:
    {VOLUME_PATH}/dt=YYYY-MM-DD/events_<uuid>.ndjson

Auto Loader's ``cloudFiles`` source treats every file under the watched
prefix as a new file regardless of subfolder, so partitioning by
``dt=...`` is purely organisational — it makes the landing zone easy to
inspect by hand without changing how Bronze ingests it.

A ``DATABRICKS_DRYRUN=true`` env var disables the network call and
appends to a local NDJSON file instead, useful for offline development.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


class DatabricksConfigError(RuntimeError):
    """Raised when required environment variables are missing."""


class DatabricksUploadError(RuntimeError):
    """Raised when the Files API rejects an upload."""


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise DatabricksConfigError(
            f"Missing required environment variable: {name}. "
            f"Copy web/config/.env.example to .env and fill it in."
        )
    return value


class DatabricksVolumeClient:
    """Thin wrapper around the Volumes Files API for NDJSON uploads.

    Thread-safe: the underlying ``httpx.Client`` is shared and the dry-run
    file write is guarded by a lock so concurrent FastAPI requests do not
    interleave half-written lines.
    """

    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        volume_path: str | None = None,
        dry_run: bool | None = None,
        dry_run_dir: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.host = (host or _env("DATABRICKS_HOST")).rstrip("/")
        self.volume_path = (volume_path or _env("VOLUME_PATH")).rstrip("/")

        env_dry = os.getenv("DATABRICKS_DRYRUN", "").lower() in ("1", "true", "yes")
        self.dry_run = env_dry if dry_run is None else dry_run

        # Token only required for live uploads.
        self.token = token if token is not None else os.getenv("DATABRICKS_TOKEN", "")
        if not self.dry_run and not self.token:
            raise DatabricksConfigError(
                "DATABRICKS_TOKEN is required when DATABRICKS_DRYRUN is not set."
            )

        self.dry_run_dir = Path(
            dry_run_dir or os.getenv("DRYRUN_DIR", "./dryrun_events")
        )
        if self.dry_run:
            self.dry_run_dir.mkdir(parents=True, exist_ok=True)

        self._client = httpx.Client(
            timeout=timeout,
            headers={"Authorization": f"Bearer {self.token}"} if self.token else {},
        )
        self._dry_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def upload_events(
        self,
        events: Iterable[Mapping[str, object]],
        partition_date: str | None = None,
    ) -> dict[str, str | int]:
        """Serialise ``events`` as NDJSON and PUT them to the volume.

        One call → one file. The caller decides batching granularity.

        Returns a small summary dict suitable for logging or returning
        from an HTTP handler.
        """
        events = list(events)
        if not events:
            return {"status": "noop", "events": 0, "path": ""}

        if partition_date is None:
            partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        ndjson = "\n".join(json.dumps(e, separators=(",", ":")) for e in events) + "\n"
        body = ndjson.encode("utf-8")

        rel_path = f"dt={partition_date}/events_{uuid.uuid4().hex}.ndjson"
        full_path = f"{self.volume_path}/{rel_path}"

        if self.dry_run:
            return self._write_local(rel_path, body, len(events))

        return self._put_volume(full_path, body, len(events))

    def health(self) -> dict[str, object]:
        """Lightweight check for /health endpoints."""
        return {
            "host": self.host,
            "volume_path": self.volume_path,
            "dry_run": self.dry_run,
        }

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _put_volume(self, full_path: str, body: bytes, n_events: int) -> dict[str, str | int]:
        url = f"{self.host}/api/2.0/fs/files{full_path}"
        try:
            resp = self._client.put(
                url,
                params={"overwrite": "true"},
                content=body,
                headers={"Content-Type": "application/octet-stream"},
            )
        except httpx.HTTPError as exc:
            raise DatabricksUploadError(f"Network error uploading to {url}: {exc}") from exc

        if resp.status_code >= 400:
            raise DatabricksUploadError(
                f"Files API rejected upload: {resp.status_code} {resp.text}"
            )

        logger.info("Uploaded %d events → %s", n_events, full_path)
        return {
            "status": "ok",
            "events": n_events,
            "path": full_path,
            "bytes": len(body),
        }

    def _write_local(self, rel_path: str, body: bytes, n_events: int) -> dict[str, str | int]:
        target = self.dry_run_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._dry_lock:
            target.write_bytes(body)
        logger.info("DRYRUN: wrote %d events → %s", n_events, target)
        return {
            "status": "ok",
            "events": n_events,
            "path": str(target),
            "bytes": len(body),
            "dry_run": True,
        }
