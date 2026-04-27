"""Tests for the YAML config loader and env-var override mechanism."""
from __future__ import annotations

from pathlib import Path

from src.common.config import load_config


def test_load_pipeline_config_has_expected_shape():
    cfg = load_config("pipeline")
    assert cfg["catalog"] == "main"
    assert "bronze" in cfg["schemas"]
    assert "silver" in cfg["schemas"]
    assert "gold" in cfg["schemas"]
    assert cfg["volume"]["name"] == "landing"


def test_simulator_config_has_categories():
    cfg = load_config("simulator")
    weights = cfg["products"]["category_weights"]
    assert set(weights.keys()) == {"electronics", "appliances", "furniture"}
    assert abs(sum(weights.values()) - 1.0) < 1e-6


def test_env_override(monkeypatch):
    monkeypatch.setenv("ECOM_PIPELINE_CATALOG", "dev_main")
    monkeypatch.setenv("ECOM_PIPELINE_SESSION_INACTIVITY_MINUTES", "15")
    cfg = load_config("pipeline")
    assert cfg["catalog"] == "dev_main"
    assert cfg["session"]["inactivity_minutes"] == 15
