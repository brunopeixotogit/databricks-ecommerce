"""Configuration loader.

Reads YAML files from ``conf/`` and applies environment-variable
overrides so the same code can run locally, in dev, and in prod
without code edits. Env override pattern::

    ECOM_<FILE>_<DOTTED_KEY_UPPER>=value

Example: ``ECOM_PIPELINE_CATALOG=dev_main`` overrides ``catalog`` in
``pipeline.yml``; ``ECOM_SIMULATOR_USERS_INITIAL_POPULATION=100``
overrides ``users.initial_population`` in ``simulator.yml``.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONF_DIR = Path(__file__).resolve().parents[2] / "conf"


def load_config(name: str, conf_dir: str | Path | None = None) -> dict[str, Any]:
    """Load and return the YAML config identified by ``name`` (no extension)."""
    base = Path(conf_dir) if conf_dir else DEFAULT_CONF_DIR
    path = base / f"{name}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return _apply_env_overrides(cfg, prefix=name.upper())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _apply_env_overrides(cfg: dict[str, Any], prefix: str) -> dict[str, Any]:
    flat = _flatten(cfg)
    for key in list(flat.keys()):
        env_key = f"ECOM_{prefix}_" + key.replace(".", "_").upper()
        if env_key in os.environ:
            flat[key] = _coerce(os.environ[env_key], flat[key])
    return _unflatten(flat)


def _flatten(d: dict[str, Any], parent: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{parent}.{k}" if parent else k
        if isinstance(v, dict):
            out.update(_flatten(v, key))
        else:
            out[key] = v
    return out


def _unflatten(flat: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in flat.items():
        cursor = out
        parts = key.split(".")
        for p in parts[:-1]:
            cursor = cursor.setdefault(p, {})
        cursor[parts[-1]] = value
    return out


def _coerce(raw: str, ref: Any) -> Any:
    if isinstance(ref, bool):
        return raw.lower() in {"1", "true", "yes", "y"}
    if isinstance(ref, int) and not isinstance(ref, bool):
        return int(raw)
    if isinstance(ref, float):
        return float(raw)
    return raw
