"""Typed domain model shared across the agent.

These mirror the wire contracts in contracts/v1 but are the in-process types the
reconciler, drivers, and supervisor operate on. Parsing from wire dicts happens
at the control-client boundary; internal code never touches raw dicts.
"""

from __future__ import annotations

import re
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class EnginePhase(str, Enum):
    PENDING = "Pending"
    PULLING = "Pulling"
    STARTING = "Starting"
    WARMING = "Warming"
    READY = "Ready"
    DRAINING = "Draining"
    STOPPED = "Stopped"
    FAILED = "Failed"
    QUARANTINED = "Quarantined"
    ORPHANED = "Orphaned"


class DesiredPhase(str, Enum):
    RUNNING = "Running"
    DRAINING = "Draining"
    STOPPED = "Stopped"


class EngineSpec(BaseModel):
    model_config = {"extra": "forbid"}
    kind: str
    image_digest: str
    version: str = ""


class ArtifactRef(BaseModel):
    model_config = {"extra": "forbid"}
    artifact_id: str
    revision: str
    manifest_digest: str
    engine_image_digest: str


class Resources(BaseModel):
    model_config = {"extra": "forbid"}
    device_ids: list[str] = Field(default_factory=list)
    memory_limit_bytes: int = 0
    port: int


class RestartPolicy(BaseModel):
    model_config = {"extra": "forbid"}
    max_attempts: int = 5
    backoff_seconds: int = 30


class EnginePolicy(BaseModel):
    model_config = {"extra": "forbid"}
    restart: RestartPolicy = Field(default_factory=RestartPolicy)
    drain_timeout_seconds: int = 120


class Assignment(BaseModel):
    model_config = {"extra": "forbid"}
    engine_id: str
    node_id: str
    generation: int
    spec_revision: int = 0
    desired_phase: DesiredPhase
    engine: EngineSpec
    artifact: ArtifactRef
    resources: Resources
    parameters: dict = Field(default_factory=dict)
    policy: EnginePolicy = Field(default_factory=EnginePolicy)

    def replacement_key(self) -> tuple:
        """Fields whose change forces stop-and-replace rather than in-place update
        (DESIGN.md 11): image digest, artifact digest, device set, port."""
        return (
            self.engine.image_digest,
            self.artifact.manifest_digest,
            tuple(sorted(self.resources.device_ids)),
            self.resources.port,
        )


class JobPhase(str, Enum):
    PENDING = "Pending"
    STARTING = "Starting"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


JOB_TERMINAL = frozenset({JobPhase.SUCCEEDED, JobPhase.FAILED, JobPhase.CANCELLED})

# "<registry>/<repo>@sha256:<64 hex>". Jobs are launched by digest only: a tag
# is mutable, so it can never identify what actually ran.
IMAGE_REF = re.compile(r"^[a-z0-9]([a-z0-9._:-]*[a-z0-9])?(/[a-z0-9]([a-z0-9._-]*[a-z0-9])?)+@sha256:[0-9a-f]{64}$")


class JobAssignment(BaseModel):
    """One replica of a batch job or long-running service (DESIGN.md 25).

    Unlike an engine assignment, a job carries its own arguments: that is the
    product. The safety boundary is therefore the local job policy (image
    allow-list, devices, port range) and container hardening, not the driver."""

    model_config = {"extra": "forbid"}
    job_id: str
    replica: int = Field(default=0, ge=0)
    kind: str = "job"  # job (runs to completion) | service (long-running, has a port)
    image: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    device_ids: list[str] = Field(default_factory=list)
    memory_limit_bytes: int = Field(default=0, ge=0)
    port: int = Field(default=0, ge=0, le=65535)  # 0 = no published port
    timeout_seconds: int = Field(default=0, ge=0)  # 0 = no node-side limit
    desired_phase: DesiredPhase = DesiredPhase.RUNNING

    @field_validator("kind")
    @classmethod
    def _known_kind(cls, v: str) -> str:
        if v not in ("job", "service"):
            raise ValueError(f"job kind must be 'job' or 'service', got {v!r}")
        return v

    @field_validator("image")
    @classmethod
    def _pinned_image(cls, v: str) -> str:
        if not IMAGE_REF.match(v):
            raise ValueError("job image must be a digest-pinned reference: <registry>/<repo>@sha256:<digest>")
        return v

    @property
    def key(self) -> str:
        return f"{self.job_id}.{self.replica}"

    @property
    def image_digest(self) -> str:
        return self.image.rsplit("@", 1)[1]

    @property
    def registry(self) -> str:
        return self.image.split("/", 1)[0]


class DesiredSnapshot(BaseModel):
    model_config = {"extra": "forbid"}
    node_id: str
    generation: int
    snapshot_hash: str
    assignments: list[Assignment] = Field(default_factory=list)
    jobs: list[JobAssignment] = Field(default_factory=list)

    def jobs_by_key(self) -> dict[str, JobAssignment]:
        return {j.key: j for j in self.jobs}

    def by_engine_id(self) -> dict[str, Assignment]:
        return {a.engine_id: a for a in self.assignments}
