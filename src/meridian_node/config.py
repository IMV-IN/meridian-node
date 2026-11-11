"""Agent configuration. Fails closed on empty or invalid input.

Acceptance checklist (DESIGN.md section 23): empty or invalid configuration
must fail closed with an actionable error.
"""

from __future__ import annotations

import ipaddress
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from .models import valid_host

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
    # Operator-declared NVLink/affinity groups: device_id -> group id. Reported so
    # the control plane can place a multi-GPU engine on NVLink-connected devices
    # (Phase 5). Empty => no affinity constraint.
    device_nvlink_groups: dict[str, str] = Field(default_factory=dict)
    engine_host: str = "127.0.0.1"
    helper_socket_path: str = "/run/meridian-node/helper.sock"
    # --- batch jobs and services (DESIGN.md 25) -------------------------
    # Off by default. A job runs a tenant-supplied, digest-pinned container
    # image with its own arguments, so the host owner opts in here.
    jobs_enabled: bool = False
    # Image digests jobs may run. Ignored when job_allow_any_image is true.
    allowed_job_image_digests: list[str] = Field(default_factory=list)
    # Accept any digest-pinned image (a marketplace host renting whole GPUs).
    # allowed_job_registries still applies when it is non-empty.
    job_allow_any_image: bool = False
    allowed_job_registries: list[str] = Field(default_factory=list)
    job_port_range: list[int] = Field(default=[9000, 9100])  # [start, stop)
    # Hard ceiling on one job's runtime on this host; 0 = no host-side ceiling.
    job_max_runtime_seconds: int = Field(default=0, ge=0)
    # IP address published job ports bind to. Empty = engine_host, the address
    # the node advertises (which must then be an IP literal). Never all
    # interfaces unless set to 0.0.0.0 on purpose (DESIGN.md 25.3).
    job_bind_address: str = ""
    # Memory cap for a job that asks for none, and the largest cap a job may ask for.
    job_memory_default_bytes: int = Field(default=32 * 1024**3, gt=0)
    job_memory_max_bytes: int = Field(default=256 * 1024**3, gt=0)
    # CPUs one job container may use (docker --cpus); 0 = no limit.
    job_cpus: float = Field(default=0.0, ge=0)

    @field_validator("port_range", "job_port_range")
    @classmethod
    def _valid_range(cls, v: list[int]) -> list[int]:
        if len(v) != 2 or v[0] >= v[1]:
            raise ValueError(f"port ranges must be [start, stop) with start < stop, got {v}")
        return v

    @field_validator("job_bind_address")
    @classmethod
    def _valid_bind_address(cls, v: str) -> str:
        if v:
            ipaddress.ip_address(v)  # ValueError: "... does not appear to be an IPv4 or IPv6 address"
        return v

    @field_validator("engine_host")
    @classmethod
    def _valid_engine_host(cls, v: str) -> str:
        if not valid_host(v):
            raise ValueError("engine_host must be an IP address or a host name of at most 253 characters")
        return v

    @model_validator(mode="after")
    def _job_limits(self) -> "Policy":
        if self.job_memory_default_bytes > self.job_memory_max_bytes:
            raise ValueError("job_memory_default_bytes must not exceed job_memory_max_bytes")
        if self.jobs_enabled:
            try:
                ipaddress.ip_address(self.effective_job_bind_address)
            except ValueError:
                raise ValueError(
                    "jobs need an IP address to publish ports on: set policy.job_bind_address "
                    "(engine_host is not an IP address)"
                ) from None
        return self

    @property
    def effective_job_bind_address(self) -> str:
        return self.job_bind_address or self.engine_host


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

    def jobs_allowed(self) -> bool:
        """Jobs need managed lifecycle control plus the separate jobs switch."""
        return self.managed_allowed() and self.policy.jobs_enabled

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
