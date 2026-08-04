"""Reconciliation (DESIGN.md 11).

Applies a complete desired snapshot for one node: validates generation
monotonicity, discovers running processes, advances each engine one bounded step
via the supervisor, and drives deletions (assignments absent from a newer
snapshot) through Draining before Stopped. Produces the observed-state payload
the agent reports back.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .models import Assignment, DesiredPhase, DesiredSnapshot, EnginePhase
from .state import StateStore
from .supervisor import EngineSupervisor


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Reconciler:
    def __init__(self, node_id: str, supervisor: EngineSupervisor, state: StateStore, now=_utcnow):
        self.node_id = node_id
        self.supervisor = supervisor
        self.state = state
        self.now = now
        self._last_assignments: dict[str, Assignment] = {}

    def apply(self, snapshot: DesiredSnapshot, stop_authorized: Optional[set[str]] = None) -> dict:
        if snapshot.node_id != self.node_id:
            raise ValueError(f"snapshot for {snapshot.node_id} applied on {self.node_id}")
        if snapshot.generation < self.state.accepted_generation():
            raise ValueError("refusing to apply a lower desired generation")
        stop_authorized = stop_authorized or set()

        desired = snapshot.by_engine_id()

        # Deletions: engines we managed before that are gone from this snapshot.
        deleted = {
            eid: self._synthetic_stop(a)
            for eid, a in self._last_assignments.items()
            if eid not in desired
        }
        effective = {**desired, **deleted}

        self.supervisor.discover(effective)

        for engine_id, assignment in effective.items():
            can_stop = engine_id in stop_authorized
            self.supervisor.reconcile_engine(assignment, can_stop=can_stop)

        # Drop fully-stopped deletions from tracking; keep live desired assignments.
        self._last_assignments = dict(desired)
        for eid, assignment in deleted.items():
            if self.supervisor.state(eid).phase != EnginePhase.STOPPED:
                self._last_assignments[eid] = assignment  # keep draining until stopped

        self.state.set_accepted_generation(snapshot.generation)
        return self._observation(snapshot.generation, effective)

    def _synthetic_stop(self, assignment: Assignment) -> Assignment:
        return assignment.model_copy(update={"desired_phase": DesiredPhase.STOPPED})

    def _observation(self, generation: int, assignments: dict[str, Assignment]) -> dict:
        now = self.now()
        engines = []
        for engine_id, assignment in assignments.items():
            st = self.supervisor.state(engine_id)
            endpoint = ""
            loaded = None
            if st.phase == EnginePhase.READY and st.handle is not None:
                endpoint = f"https://{self.supervisor.engine_host}:{st.handle.port}"
                loaded = assignment.artifact.manifest_digest
            engines.append({
                "engine_id": engine_id,
                "phase": st.phase.value,
                "restart_count": st.restart_count,
                "loaded_artifact_digest": loaded,
                "endpoint": endpoint,
                # Model the gateway routes on; operators may override via parameters.
                "model": assignment.parameters.get("model") or assignment.artifact.artifact_id,
                "queue_depth": st.queue_depth,
                "conditions": [st.condition(now)],
            })
        orphan_conditions = [
            {"type": "OrphanedProcess", "status": "True", "reason": "UnknownOwnershipMarker",
             "message": o.runtime_id, "observed_at": now.isoformat().replace("+00:00", "Z")}
            for o in self.supervisor.orphans
        ]
        return {
            "node_id": self.node_id,
            "observed_generation": generation,
            "engines": engines,
            "conditions": orphan_conditions,
            "capacity": self.supervisor.capacity_report(),
        }
