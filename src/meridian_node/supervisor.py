"""Engine supervision (DESIGN.md 9.2, 11, 14).

Drives one engine per assignment through the phase state machine using a driver,
a runtime adapter, the artifact store, a readiness prober, the operation
journal, and the reservation ledger. Every launch and stop is journaled and
idempotent. Restart uses bounded backoff and ends in quarantine, never a
crash-loop. Processes are adopted only on an exact ownership + digest + device
match; unmatched processes are marked Orphaned and left running.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

from .artifacts import ArtifactStore, verify_manifest
from .drivers import Driver, ReadinessSpec, get_driver
from .journal import COMMITTED, OBSERVED, STARTED, Journal
from .models import Assignment, DesiredPhase, EnginePhase
from .reservation import ReservationError, ReservationLedger
from .runtime import EngineHandle, Runtime, RuntimeStatus
from .statemachine import assert_transition


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ManifestSource(Protocol):
    def get(self, manifest_digest: str, revision: str) -> tuple[dict, str]:
        """Return (manifest_dict, source_uri) for a manifest digest."""
        ...


class Prober(Protocol):
    def probe(self, host: str, port: int, spec: ReadinessSpec) -> bool: ...


@dataclass
class EngineState:
    engine_id: str
    phase: EnginePhase = EnginePhase.PENDING
    handle: Optional[EngineHandle] = None
    restart_count: int = 0
    next_retry_at: Optional[datetime] = None
    reason: str = ""
    message: str = ""

    def condition(self, observed_at: datetime) -> dict:
        return {
            "type": "EnginePhase",
            "status": "True",
            "reason": self.reason or self.phase.value,
            "message": self.message,
            "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
        }


def _marker(node_id: str, engine_id: str, operation_id: str) -> str:
    return f"{node_id}:{engine_id}:{operation_id}"


def _request_digest(req) -> str:
    canonical = json.dumps(req, sort_keys=True, separators=(",", ":"), default=str).encode()
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


class EngineSupervisor:
    def __init__(
        self,
        node_id: str,
        runtime: Runtime,
        artifacts: ArtifactStore,
        manifests: ManifestSource,
        prober: Prober,
        journal: Journal,
        reservations: ReservationLedger,
        trusted_keys: dict[str, bytes],
        fetcher,
        engine_host: str = "127.0.0.1",
        nvlink_groups: Optional[dict] = None,
        now=_utcnow,
    ):
        self.node_id = node_id
        self.runtime = runtime
        self.artifacts = artifacts
        self.manifests = manifests
        self.prober = prober
        self.journal = journal
        self.reservations = reservations
        self.trusted_keys = trusted_keys
        self._fetcher = fetcher
        self.engine_host = engine_host
        self.nvlink_groups = nvlink_groups or {}
        self.now = now
        self.fencing_epoch = 0
        self._states: dict[str, EngineState] = {}
        self.orphans: list[RuntimeStatus] = []

    def state(self, engine_id: str) -> EngineState:
        return self._states.setdefault(engine_id, EngineState(engine_id=engine_id))

    def capacity_report(self) -> dict:
        """Allocatable capacity + placement signals the control plane consumes
        (P3/P5). VRAM from the reservation ledger's committed budget; free ports
        from the range; held artifacts for locality; running-engine load for
        load-aware placement; NVLink groups for multi-GPU affinity."""
        reserved = self.reservations.reserved_ports()
        free_ports = sum(1 for p in self.reservations.port_range if p not in reserved)
        running = sum(1 for st in self._states.values() if st.phase == EnginePhase.READY)
        return {
            "allocatable_vram_bytes": self.reservations.allocatable(),
            "free_ports": free_ports,
            "held_artifacts": self.artifacts.held(),
            "running_engines": running,
            "nvlink_groups": dict(self.nvlink_groups),
        }

    # --- discovery / adoption ------------------------------------------
    def discover(self, assignments: dict[str, Assignment]) -> None:
        """Classify running processes as adopted or orphaned (DESIGN.md 11.2)."""
        self.orphans = []
        for status in self.runtime.list_all():
            if not status.alive:
                continue
            owner_node, owner_engine, _ = (status.ownership_marker.split(":", 2) + ["", "", ""])[:3]
            assignment = assignments.get(owner_engine)
            adoptable = (
                owner_node == self.node_id
                and assignment is not None
                and status.image_digest == assignment.engine.image_digest
                and tuple(status.device_ids) == tuple(assignment.resources.device_ids)
            )
            if adoptable:
                st = self.state(owner_engine)
                if st.handle is None:
                    st.handle = EngineHandle(
                        engine_id=owner_engine, runtime_id=status.runtime_id,
                        image_digest=status.image_digest, ownership_marker=status.ownership_marker,
                        device_ids=tuple(status.device_ids), port=status.port,
                    )
                    st.phase = EnginePhase.WARMING  # re-verify readiness before Ready
            else:
                self.orphans.append(status)

    # --- lifecycle step ------------------------------------------------
    def reconcile_engine(self, assignment: Assignment, can_stop: bool = False) -> EngineState:
        """Advance one engine by at most one bounded operation, idempotently."""
        st = self.state(assignment.engine_id)
        now = self.now()

        # Liveness first: a dead handle is a failure regardless of desired phase.
        if st.handle is not None and st.phase in (EnginePhase.WARMING, EnginePhase.READY):
            status = self.runtime.inspect(st.handle.runtime_id)
            if status is None or not status.alive:
                self._fail(st, assignment, "ProcessExited", "engine process is not alive")
                return st

        desired = assignment.desired_phase
        if desired in (DesiredPhase.DRAINING, DesiredPhase.STOPPED):
            return self._drive_stop(st, assignment, can_stop)

        # desired == RUNNING
        if st.phase == EnginePhase.QUARANTINED:
            return st
        if st.phase == EnginePhase.FAILED:
            if st.next_retry_at and now < st.next_retry_at:
                return st
            self._transition(st, EnginePhase.PENDING)
        if st.phase in (EnginePhase.PENDING, EnginePhase.STOPPED):
            self._start(st, assignment)
        elif st.phase == EnginePhase.WARMING:
            self._try_ready(st, assignment)
        return st

    def _start(self, st: EngineState, assignment: Assignment) -> None:
        driver = get_driver(assignment.engine.kind)
        gpus = [{"device_id": d} for d in assignment.resources.device_ids]  # inventory checked upstream
        try:
            driver.validate(assignment, gpus)
            self.reservations.reserve(
                assignment.engine_id, assignment.resources.device_ids,
                assignment.resources.memory_limit_bytes, assignment.resources.port,
            )
        except (ReservationError, ValueError) as e:
            self._fail(st, assignment, "LocalAdmissionRejected", str(e))
            return

        # Pulling: resolve + verify signed manifest, materialize artifact.
        try:
            self._transition(st, EnginePhase.PULLING)
            artifact_path = self._materialize(assignment)
        except Exception as e:  # verification/transfer failure
            self._fail(st, assignment, "ArtifactVerificationFailed", str(e))
            self.reservations.release(assignment.engine_id)
            return

        # Starting: journaled launch.
        self._transition(st, EnginePhase.STARTING)
        operation_id = f"launch:{assignment.engine_id}:{assignment.generation}:{assignment.spec_revision}"
        marker = _marker(self.node_id, assignment.engine_id, operation_id)
        req = driver.build_launch(assignment, str(artifact_path), marker)
        digest = _request_digest(req.__dict__)
        prior = self.journal.plan(operation_id, digest, assignment.engine_id, self.fencing_epoch, "launch")
        if prior == COMMITTED:
            # Already launched in a previous life; adopt via discovery instead of relaunching.
            row = self.journal.inspect(operation_id)
            if row and row.result:
                existing = self.runtime.inspect(row.result)
                if existing and existing.alive:
                    st.handle = EngineHandle(
                        engine_id=assignment.engine_id, runtime_id=existing.runtime_id,
                        image_digest=existing.image_digest, ownership_marker=existing.ownership_marker,
                        device_ids=tuple(existing.device_ids), port=existing.port,
                    )
                    self._transition(st, EnginePhase.WARMING)
                    return
        self.journal.mark(operation_id, STARTED)
        handle = self.runtime.launch(req)
        self.journal.mark(operation_id, OBSERVED, result=handle.runtime_id)
        self.journal.mark(operation_id, COMMITTED)
        st.handle = handle
        self._transition(st, EnginePhase.WARMING)

    def _materialize(self, assignment: Assignment):
        ref = assignment.artifact
        if self.artifacts.has(ref.manifest_digest):
            self.artifacts.pin(ref.manifest_digest)
            return self.artifacts.path_for(ref.manifest_digest)
        manifest_dict, source_uri = self.manifests.get(ref.manifest_digest, ref.revision)
        manifest = verify_manifest(manifest_dict, self.trusted_keys, ref.manifest_digest)
        if manifest.engine_image_digest != assignment.engine.image_digest:
            raise ValueError("manifest engine image digest does not match assignment")
        if manifest.artifact_id != ref.artifact_id or manifest.revision != ref.revision:
            raise ValueError("manifest artifact_id/revision does not match assignment")
        path = self.artifacts.materialize(manifest, ref.manifest_digest, self._fetcher, source_uri)
        self.artifacts.pin(ref.manifest_digest)
        return path

    def _try_ready(self, st: EngineState, assignment: Assignment) -> None:
        driver: Driver = get_driver(assignment.engine.kind)
        spec = driver.readiness(assignment)
        assert st.handle is not None
        if self.prober.probe(self.engine_host, st.handle.port, spec):
            self._transition(st, EnginePhase.READY, reason="ModelLoaded")

    def _drive_stop(self, st: EngineState, assignment: Assignment, can_stop: bool) -> EngineState:
        if st.phase in (EnginePhase.STOPPED, EnginePhase.PENDING) and st.handle is None:
            self._transition(st, EnginePhase.STOPPED)
            return st
        if st.phase != EnginePhase.DRAINING:
            self._transition(st, EnginePhase.DRAINING, reason="DrainRequested")
        if can_stop and st.handle is not None:
            operation_id = f"stop:{assignment.engine_id}:{st.handle.runtime_id}"
            digest = _request_digest({"stop": st.handle.runtime_id})
            self.journal.plan(operation_id, digest, assignment.engine_id, self.fencing_epoch, "stop")
            self.journal.mark(operation_id, STARTED)
            self.runtime.stop(st.handle.runtime_id, assignment.policy.drain_timeout_seconds)
            self.journal.mark(operation_id, COMMITTED, result=st.handle.runtime_id)
            self.reservations.release(assignment.engine_id)
            st.handle = None
            self._transition(st, EnginePhase.STOPPED, reason="Drained")
        return st

    def _fail(self, st: EngineState, assignment: Assignment, reason: str, message: str) -> None:
        st.restart_count += 1
        self._transition(st, EnginePhase.FAILED, reason=reason, message=message)
        st.handle = None
        self.reservations.release(assignment.engine_id)
        policy = assignment.policy.restart
        if st.restart_count > policy.max_attempts:
            self._transition(st, EnginePhase.QUARANTINED, reason="RestartBudgetExhausted", message=message)
            return
        backoff = policy.backoff_seconds * (2 ** (st.restart_count - 1))
        st.next_retry_at = self.now() + timedelta(seconds=backoff)

    def operator_reset(self, engine_id: str) -> None:
        st = self.state(engine_id)
        if st.phase == EnginePhase.QUARANTINED:
            st.restart_count = 0
            st.next_retry_at = None
            self._transition(st, EnginePhase.PENDING, reason="OperatorReset")

    def _transition(self, st: EngineState, target: EnginePhase, reason: str = "", message: str = "") -> None:
        assert_transition(st.phase, target)
        st.phase = target
        if reason:
            st.reason = reason
        st.message = message
