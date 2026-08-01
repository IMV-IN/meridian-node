"""Typed domain model shared across the agent.

These mirror the wire contracts in contracts/v1 but are the in-process types the
reconciler, drivers, and supervisor operate on. Parsing from wire dicts happens
at the control-client boundary; internal code never touches raw dicts.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


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


class DesiredSnapshot(BaseModel):
    model_config = {"extra": "forbid"}
    node_id: str
    generation: int
    snapshot_hash: str
    assignments: list[Assignment] = Field(default_factory=list)

    def by_engine_id(self) -> dict[str, Assignment]:
        return {a.engine_id: a for a in self.assignments}
