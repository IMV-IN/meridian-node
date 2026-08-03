"""Agent configuration. Fails closed on empty or invalid input.

Acceptance checklist (DESIGN.md section 23): empty or invalid configuration
must fail closed with an actionable error.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

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


class AirGapConfig(BaseModel):
    """Offline mode inputs (DESIGN.md 6.4). `node_id` addresses this node; the
    signed desired-state bundle placed in `inbox_dir` is the trust anchor that
    replaces the mTLS session. `authority_keys` (key_id -> base64url Ed25519
    pubkey) are the 'what should run' authority, distinct from the artifact
    publisher's `trusted_signing_keys`. Observations spool to `outbox_dir`."""

    model_config = {"extra": "forbid"}

    node_id: str = ""
    inbox_dir: Path = Path("/var/lib/meridian-node/airgap/inbox")
    outbox_dir: Path = Path("/var/lib/meridian-node/airgap/outbox")
    authority_keys: dict[str, str] = Field(default_factory=dict)


class KubernetesConfig(BaseModel):
    """kubernetes-integrated mode inputs (DESIGN.md 6.3). The agent reports
    inventory and *observes* engines the cluster scheduled on this node — it never
    launches them (the k8s control plane owns lifecycle). `node_name` filters pods
    to this host (defaults to $MERIDIAN_K8S_NODE_NAME via the Downward API)."""

    model_config = {"extra": "forbid"}

    namespace: str = "default"
    label_selector: str = "app.kubernetes.io/managed-by=meridian"
    node_name: str = ""
    engine_port_annotation: str = "meridian.io/engine-port"
    model_annotation: str = "meridian.io/model"


class Config(BaseModel):
    model_config = {"extra": "forbid"}

    # Required for every mode except air-gapped, which has no control channel
    # (enforced by _check_mode_inputs below).
    control_plane_url: str = ""
    # Default mode is the safest: observe-only cannot start/stop engines.
    mode: str = "observe-only"
    state_dir: Path = Path("/var/lib/meridian-node")
    display_name: str = ""
    labels: dict[str, str] = Field(default_factory=dict)
    heartbeat_interval_seconds: int = Field(default=10, ge=1)
    lease_ttl_seconds: int = Field(default=30, ge=1)
    policy: Policy = Field(default_factory=Policy)
    air_gap: AirGapConfig = Field(default_factory=AirGapConfig)
    kubernetes: KubernetesConfig = Field(default_factory=KubernetesConfig)

    @field_validator("mode")
    @classmethod
    def _known_mode(cls, v: str) -> str:
        if v not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {v!r}")
        return v

    @model_validator(mode="after")
    def _check_mode_inputs(self) -> "Config":
        if self.mode == "air-gapped":
            if not self.air_gap.node_id:
                raise ValueError("air-gapped mode requires air_gap.node_id")
            if not self.air_gap.authority_keys:
                raise ValueError("air-gapped mode requires at least one air_gap.authority_keys entry")
        elif not self.control_plane_url:
            raise ValueError("control_plane_url is required")
        return self

    def managed_allowed(self) -> bool:
        """Managed lifecycle requires an engine-launching mode plus the local
        policy switch. Either off means observe-only behavior."""
        return self.mode in ("managed-host", "air-gapped") and self.policy.management_enabled


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
