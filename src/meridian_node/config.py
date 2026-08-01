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


class Policy(BaseModel):
    """Local, root-owned safety policy (DESIGN.md 15.1). An independent
    constraint the agent enforces even against an authenticated control plane.
    `management_enabled=false` is the disable-management kill switch: it stops
    lifecycle mutations without deleting evidence or killing running engines."""

    model_config = {"extra": "forbid"}

    management_enabled: bool = False
    allowed_image_digests: list[str] = Field(default_factory=list)
    trusted_signing_keys: dict[str, str] = Field(default_factory=dict)  # key_id -> base64url pubkey
    allowed_source_hosts: list[str] = Field(default_factory=list)
    port_range: list[int] = Field(default=[8000, 8100])  # [start, stop)
    device_capacity_bytes: dict[str, int] = Field(default_factory=dict)
    engine_host: str = "127.0.0.1"
    helper_socket_path: str = "/run/meridian-node/helper.sock"

    @field_validator("port_range")
    @classmethod
    def _valid_range(cls, v: list[int]) -> list[int]:
        if len(v) != 2 or v[0] >= v[1]:
            raise ValueError(f"port_range must be [start, stop) with start < stop, got {v}")
        return v


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
    policy: Policy = Field(default_factory=Policy)

    @field_validator("mode")
    @classmethod
    def _known_mode(cls, v: str) -> str:
        if v not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {v!r}")
        return v

    def managed_allowed(self) -> bool:
        """Managed lifecycle requires both managed-host mode and the local
        policy switch. Either off means observe-only behavior."""
        return self.mode == "managed-host" and self.policy.management_enabled


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
