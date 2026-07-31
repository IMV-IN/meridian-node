"""Agent configuration. Fails closed on empty or invalid input.

Acceptance checklist (DESIGN.md section 23): empty or invalid configuration
must fail closed with an actionable error.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator

MODES = ("observe-only", "managed-host", "kubernetes-integrated", "air-gapped")


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid. Actionable message."""


class Config(BaseModel):
    model_config = {"extra": "forbid"}

    control_plane_url: str = Field(min_length=1)
    # Default mode is the safest: observe-only cannot start/stop engines.
    mode: str = "observe-only"
    state_dir: Path = Path("/var/lib/meridian-node")
    display_name: str = ""
    labels: dict[str, str] = Field(default_factory=dict)
    heartbeat_interval_seconds: int = Field(default=10, ge=1)
    lease_ttl_seconds: int = Field(default=30, ge=1)

    @field_validator("mode")
    @classmethod
    def _known_mode(cls, v: str) -> str:
        if v not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {v!r}")
        return v


def load_config(path: str | Path) -> Config:
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"config file not found: {p}")
    text = p.read_text()
    if not text.strip():
        raise ConfigError(f"config file is empty: {p}")
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ConfigError(f"config file is not valid YAML: {p}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"config must be a mapping, got {type(raw).__name__}: {p}")
    try:
        return Config(**raw)
    except ValidationError as e:
        raise ConfigError(f"invalid config {p}:\n{e}") from e
