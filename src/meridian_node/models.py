"""Typed domain model shared across the agent.

These mirror the wire contracts in contracts/v1 but are the in-process types the
reconciler, drivers, and supervisor operate on. Parsing from wire dicts happens
at the control-client boundary; internal code never touches raw dicts.
"""

from __future__ import annotations

import ipaddress
import re
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


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
# Matched with fullmatch: `$` alone also accepts a trailing newline. The first
# character is alphanumeric, so the reference can never be read as a docker option.
IMAGE_REF = re.compile(r"[a-z0-9]([a-z0-9._:-]*[a-z0-9])?(/[a-z0-9]([a-z0-9._-]*[a-z0-9])?)+@sha256:[0-9a-f]{64}")
MAX_IMAGE_REF = 512
# NVML UUIDs (GPU-…, MIG-…) and legacy MIG names (MIG-GPU-…/1/0). No comma or
# quote: device lists are passed to docker as CSV.
DEVICE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}")

# Identifiers the node puts into container labels, ownership markers and
# file names. The control plane issues `job_<hex>`.
JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

# --- job arguments and environment (DESIGN.md 25.3) -------------------------
# The same checks run in the agent (this model) and again in the root helper,
# which trusts nothing the agent sends.
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
MAX_JOB_ARGS = 256
MAX_JOB_ENV = 128
MAX_JOB_STRING = 8192  # bytes, per argument or value
MAX_JOB_TOTAL = 256 * 1024  # bytes, all arguments and values together
# The node sets GPU visibility for every job container. A job may not set or
# override these names: with the nvidia runtime as the host default, the
# container's NVIDIA_VISIBLE_DEVICES decides which GPUs it gets (H4).
# NVIDIA_* is reserved as a family because the container toolkit reads several
# of its members (MIG, IMEX channels, requirements); NVIDIA_TF32_OVERRIDE is a
# numerics switch read by CUDA libraries, not by the toolkit.
RESERVED_ENV_NAMES = frozenset({"NVIDIA_VISIBLE_DEVICES", "NVIDIA_DRIVER_CAPABILITIES", "CUDA_VISIBLE_DEVICES"})
RESERVED_ENV_PREFIXES = ("NVIDIA_", "CUDA_MPS_")
UNRESERVED_ENV_NAMES = frozenset({"NVIDIA_TF32_OVERRIDE"})


def env_name_reserved(name: str) -> bool:
    if name in RESERVED_ENV_NAMES:
        return True
    return name.startswith(RESERVED_ENV_PREFIXES) and name not in UNRESERVED_ENV_NAMES


def _utf8_size(value: str) -> Optional[int]:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:  # lone surrogates: not text
        return None


def job_strings_error(args: Any, env: Any) -> Optional[str]:
    """Why a job's args or env are unacceptable, or None. Values are data: env
    values may not contain NUL or line breaks (they are written one per line to
    an env file), and nothing user-supplied is echoed back except a name that
    already matched ENV_NAME."""
    if not isinstance(args, list) or len(args) > MAX_JOB_ARGS:
        return f"job args must be a list of at most {MAX_JOB_ARGS} strings"
    if not isinstance(env, dict) or len(env) > MAX_JOB_ENV:
        return f"job env must be a mapping of at most {MAX_JOB_ENV} entries"
    total = 0
    for a in args:
        size = _utf8_size(a) if isinstance(a, str) else None
        if size is None or size > MAX_JOB_STRING or "\x00" in a:
            return f"job args must be strings of at most {MAX_JOB_STRING} bytes without NUL"
        total += size
    for k, v in env.items():
        if not isinstance(k, str) or not ENV_NAME.fullmatch(k):
            return "invalid environment variable name: " + repr(k)[:64]
        if env_name_reserved(k):
            return f"environment variable {k} is reserved: the node sets it"
        size = _utf8_size(v) if isinstance(v, str) else None
        if size is None or size > MAX_JOB_STRING or any(c in v for c in "\x00\r\n"):
            return f"invalid value for environment variable {k}: at most {MAX_JOB_STRING} bytes, no NUL or line breaks"
        total += size + len(k)
    if total > MAX_JOB_TOTAL:
        return f"job args and env exceed {MAX_JOB_TOTAL} bytes in total"
    return None


# RFC 1123 host name: dot-separated labels of letters, digits and inner hyphens.
_HOST_LABEL = re.compile(r"[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?")


def valid_host(host: str) -> bool:
    """An IP literal or an RFC 1123 host name of at most 253 characters: what
    the node may report as the address of its engines and job ports."""
    if not isinstance(host, str) or not host or len(host) > 253:
        return False
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    return all(_HOST_LABEL.fullmatch(label) for label in host.split("."))


def url_host(host: str) -> str:
    """`host` as it appears in a URL authority: IPv6 literals are bracketed."""
    return f"[{host}]" if ":" in host else host


class JobAssignment(BaseModel):
    """One replica of a batch job or long-running service (DESIGN.md 25).

    Unlike an engine assignment, a job carries its own arguments: that is the
    product. The safety boundary is therefore the local job policy (image
    allow-list, devices, port range) and container hardening, not the driver."""

    model_config = {"extra": "forbid"}
    job_id: str
    replica: int = Field(default=0, ge=0)
    kind: str = "job"  # job (runs to completion) | service (long-running, has a port)
    image: str = Field(max_length=MAX_IMAGE_REF)
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
        if not IMAGE_REF.fullmatch(v):
            raise ValueError("job image must be a digest-pinned reference: <registry>/<repo>@sha256:<digest>")
        return v

    @field_validator("job_id")
    @classmethod
    def _job_id(cls, v: str) -> str:
        if not JOB_ID.fullmatch(v):
            raise ValueError("job_id must be 1-128 letters, digits, '_', '.' or '-'")
        return v

    @field_validator("device_ids")
    @classmethod
    def _device_ids(cls, v: list[str]) -> list[str]:
        if len(v) > 64 or not all(DEVICE_ID.fullmatch(d) for d in v):
            raise ValueError("device_ids must be at most 64 device identifiers")
        return v

    @model_validator(mode="after")
    def _bounded_strings(self) -> "JobAssignment":
        error = job_strings_error(self.args, self.env)
        if error:
            raise ValueError(error)
        return self

    @property
    def key(self) -> str:
        return f"{self.job_id}.{self.replica}"

    @property
    def image_digest(self) -> str:
        return self.image.rsplit("@", 1)[1]

    @property
    def registry(self) -> str:
        return self.image.split("/", 1)[0]


class InvalidJob(BaseModel):
    job_id: str
    replica: int
    message: str


class DesiredSnapshot(BaseModel):
    model_config = {"extra": "forbid"}
    node_id: str
    generation: int
    snapshot_hash: str
    assignments: list[Assignment] = Field(default_factory=list)
    jobs: list[JobAssignment] = Field(default_factory=list)
    # Replicas whose spec failed validation. They never run and are reported
    # Failed, so one tenant's bad job cannot stop the node applying the rest of
    # its snapshot. Filled from `jobs` only; not part of the wire contract.
    invalid_jobs: list[InvalidJob] = Field(default_factory=list, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _isolate_invalid_jobs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        good: list[Any] = []
        bad: list[InvalidJob] = []
        for raw in data.get("jobs") or []:
            try:
                good.append(raw if isinstance(raw, JobAssignment) else JobAssignment.model_validate(raw))
            except ValidationError as e:
                job_id, replica = (raw.get("job_id"), raw.get("replica", 0)) if isinstance(raw, dict) else (None, 0)
                if isinstance(job_id, str) and JOB_ID.fullmatch(job_id) and type(replica) is int and replica >= 0:
                    err = e.errors()[0]  # msg only: never the offending input
                    where = ".".join(str(x) for x in err["loc"])
                    message = f"{where}: {err['msg']}" if where else err["msg"]
                    bad.append(InvalidJob(job_id=job_id, replica=replica, message=message[:1024]))
        return {**data, "jobs": good, "invalid_jobs": bad}

    def jobs_by_key(self) -> dict[str, JobAssignment]:
        return {j.key: j for j in self.jobs}

    def by_engine_id(self) -> dict[str, Assignment]:
        return {a.engine_id: a for a in self.assignments}
