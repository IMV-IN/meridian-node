"""Runtime adapter: the boundary that actually launches and stops engines.

The agent (non-root) never calls Docker directly. In production a `Runtime`
implementation forwards typed requests to the root helper over its Unix socket
(DESIGN.md 14.1). Tests use `FakeRuntime`, which models the same
ownership-marker and lifecycle contract in memory.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class LaunchRequest:
    engine_id: str
    image_digest: str
    args: tuple[str, ...]
    env: dict[str, str]
    device_ids: tuple[str, ...]
    artifact_mount: str  # host path, mounted read-only
    port: int
    ownership_marker: str  # "<node_id>:<engine_id>:<operation_id>"
    memory_limit_bytes: int = 0


@dataclass
class RuntimeStatus:
    runtime_id: str
    alive: bool
    image_digest: str
    ownership_marker: str
    device_ids: tuple[str, ...]
    port: int
    exit_code: Optional[int] = None


@dataclass
class EngineHandle:
    engine_id: str
    runtime_id: str
    image_digest: str
    ownership_marker: str
    device_ids: tuple[str, ...]
    port: int


class Runtime(Protocol):
    def launch(self, req: LaunchRequest) -> EngineHandle: ...
    def inspect(self, runtime_id: str) -> Optional[RuntimeStatus]: ...
    def stop(self, runtime_id: str, force_after_seconds: int) -> None: ...
    def list_all(self) -> list[RuntimeStatus]: ...


@dataclass
class _FakeContainer:
    status: RuntimeStatus
    ready: bool = False
    served_model: str = ""


class FakeRuntime:
    """In-memory runtime for tests. Enforces the ownership-marker contract and
    supports injected crashes and readiness delays (DESIGN.md 20 integration)."""

    def __init__(self):
        self._containers: dict[str, _FakeContainer] = {}
        self._counter = 0

    def launch(self, req: LaunchRequest) -> EngineHandle:
        self._counter += 1
        runtime_id = f"fake-{self._counter}"
        status = RuntimeStatus(
            runtime_id=runtime_id,
            alive=True,
            image_digest=req.image_digest,
            ownership_marker=req.ownership_marker,
            device_ids=req.device_ids,
            port=req.port,
        )
        self._containers[runtime_id] = _FakeContainer(status=status)
        return EngineHandle(
            engine_id=req.engine_id,
            runtime_id=runtime_id,
            image_digest=req.image_digest,
            ownership_marker=req.ownership_marker,
            device_ids=req.device_ids,
            port=req.port,
        )

    def inspect(self, runtime_id: str) -> Optional[RuntimeStatus]:
        c = self._containers.get(runtime_id)
        return c.status if c else None

    def stop(self, runtime_id: str, force_after_seconds: int) -> None:
        c = self._containers.get(runtime_id)
        if c:
            c.status.alive = False
            c.status.exit_code = 0

    def list_all(self) -> list[RuntimeStatus]:
        return [c.status for c in self._containers.values()]

    # --- test hooks -----------------------------------------------------
    def crash(self, runtime_id: str, exit_code: int = 1) -> None:
        c = self._containers[runtime_id]
        c.status.alive = False
        c.status.exit_code = exit_code

    def mark_ready(self, runtime_id: str, served_model: str) -> None:
        c = self._containers[runtime_id]
        c.ready = True
        c.served_model = served_model

    def orphan(self, image_digest: str, port: int, marker: str = "unknown:unknown:unknown") -> str:
        """Inject a process the agent did not launch, for adoption/orphan tests."""
        self._counter += 1
        runtime_id = f"fake-{self._counter}"
        status = RuntimeStatus(
            runtime_id=runtime_id, alive=True, image_digest=image_digest,
            ownership_marker=marker, device_ids=(), port=port,
        )
        self._containers[runtime_id] = _FakeContainer(status=status)
        return runtime_id
