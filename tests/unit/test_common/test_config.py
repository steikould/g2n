"""Tests for configuration loading."""

from __future__ import annotations

import os

from g2n.common.config import G2NSettings


def test_default_settings():
    """Default settings load without error."""
    settings = G2NSettings()
    assert settings.env == "dev"
    assert settings.s3.bronze_path.startswith("s3a://")
    assert settings.serving.port == 8000


def test_env_override(monkeypatch):
    """Environment variables override defaults."""
    monkeypatch.setenv("G2N_ENV", "staging")
    monkeypatch.setenv("G2N_SERVING_PORT", "9000")
    settings = G2NSettings()
    assert settings.env == "staging"
    assert settings.serving.port == 9000
