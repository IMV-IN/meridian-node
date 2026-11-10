"""Local reservation transaction (DESIGN.md 17.4).

Before launch the agent atomically reserves device IDs, a GPU-memory budget,
host RAM/CPU are represented as a single budget here, artifact disk, and a port.
Observed free VRAM is only a placement hint; central approval is necessary but
not sufficient. Partial reservations roll back as one unit. A reservation is
held until the process is confirmed terminated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


class ReservationError(Exception):
    """A LocalAdmissionRejected condition maps from this (DESIGN.md 17.1)."""


@dataclass
class _Reservation:
    engine_id: str
    device_ids: tuple[str, ...]
    gpu_memory_bytes: int
    port: int


@dataclass
class ReservationLedger:
    device_capacity: dict[str, int]  # device_id -> total VRAM budget the node will commit
    port_range: range
    # MIG topology: device_id -> parent physical GPU (None for a physical GPU).
    # Enables MIG parent/child exclusivity at launch (DESIGN.md 6.5, Phase 5).
    topology: dict[str, Optional[str]] = field(default_factory=dict)
    # Ports jobs may publish; separate from the engine range so a job can never
    # take an engine's port.
    job_port_range: range = range(0)
    _by_engine: dict[str, _Reservation] = field(default_factory=dict)

    def _mig_conflict(self, device_ids: list[str]) -> Optional[str]:
        """A physical GPU and its MIG child instances are mutually exclusive:
        reserving one forbids the other. Two children of the same parent are fine
        (that is what MIG is for). Empty topology => physical-only, no check."""
        if not self.topology:
            return None
        reserved = {d for r in self._by_engine.values() for d in r.device_ids}
        for dev in device_ids:
            parent = self.topology.get(dev)
            if parent is not None and parent in reserved:
                return f"device {dev} conflicts with reserved parent GPU {parent} (MIG exclusivity)"
            children = [c for c, p in self.topology.items() if p == dev]
            if any(c in reserved for c in children):
                return f"whole GPU {dev} conflicts with a reserved MIG child (MIG exclusivity)"
        return None

    def reserve(self, engine_id: str, device_ids: list[str], gpu_memory_bytes: int, port: int) -> None:
        if engine_id in self._by_engine:
            # Idempotent re-reservation with identical parameters is a no-op.
            existing = self._by_engine[engine_id]
            if existing.device_ids == tuple(device_ids) and existing.port == port:
                return
            raise ReservationError(f"engine {engine_id} already holds a different reservation")

        if not (self.port_range.start <= port < self.port_range.stop):
            raise ReservationError(f"port {port} outside configured range {self.port_range}")
        for other in self._by_engine.values():
            if other.port == port:
                raise ReservationError(f"port {port} already reserved by {other.engine_id}")

        for dev in device_ids:
            if dev not in self.device_capacity:
                raise ReservationError(f"unknown device {dev}")
            used = sum(r.gpu_memory_bytes for r in self._by_engine.values() if dev in r.device_ids)
            want = used + gpu_memory_bytes
            if want > self.device_capacity[dev]:
                raise ReservationError(f"device {dev} over-committed: {want} > {self.device_capacity[dev]}")

        conflict = self._mig_conflict(device_ids)
        if conflict:
            raise ReservationError(conflict)

        # All checks passed; commit as one unit.
        self._by_engine[engine_id] = _Reservation(engine_id, tuple(device_ids), gpu_memory_bytes, port)

    def reserve_exclusive(self, owner_id: str, device_ids: list[str], port: int = 0) -> None:
        """Reserve whole devices for a job replica (DESIGN.md 25). A job gets its
        GPUs exclusively: any existing reservation on a device rejects it, and
        while it is held the device reports zero allocatable VRAM. `port` 0
        means the job publishes no port."""
        existing = self._by_engine.get(owner_id)
        if existing is not None:
            if existing.device_ids == tuple(device_ids) and existing.port == port:
                return
            raise ReservationError(f"{owner_id} already holds a different reservation")
        if port:
            if not (self.job_port_range.start <= port < self.job_port_range.stop):
                raise ReservationError(f"port {port} outside the job port range {self.job_port_range}")
            if port in self.reserved_ports():
                raise ReservationError(f"port {port} already reserved")
        for dev in device_ids:
            if dev not in self.device_capacity:
                raise ReservationError(f"unknown device {dev}")
            holder = next((r.engine_id for r in self._by_engine.values() if dev in r.device_ids), None)
            if holder is not None:
                raise ReservationError(f"device {dev} is in use by {holder}")
        conflict = self._mig_conflict(device_ids)
        if conflict:
            raise ReservationError(conflict)
        budget = max((self.device_capacity[d] for d in device_ids), default=0)
        self._by_engine[owner_id] = _Reservation(owner_id, tuple(device_ids), budget, port)

    def free_devices(self) -> list[str]:
        """Devices nothing holds: what a job, which needs a whole GPU, can take."""
        held = {d for r in self._by_engine.values() for d in r.device_ids}
        return sorted(d for d in self.device_capacity if d not in held)

    def free_job_ports(self) -> int:
        reserved = self.reserved_ports()
        return sum(1 for p in self.job_port_range if p not in reserved)

    def release(self, engine_id: str) -> None:
        self._by_engine.pop(engine_id, None)

    def holds(self, engine_id: str) -> bool:
        return engine_id in self._by_engine

    def allocatable(self) -> dict[str, int]:
        """Authoritative per-device VRAM the node can still commit (DESIGN.md 24
        P3). The committed budget, not observed free VRAM, is the placement
        authority the control plane consumes for Phase-5 placement."""
        out: dict[str, int] = {}
        for dev, total in self.device_capacity.items():
            committed = sum(r.gpu_memory_bytes for r in self._by_engine.values() if dev in r.device_ids)
            out[dev] = max(0, total - committed)
        return out

    def reserved_ports(self) -> set[int]:
        return {r.port for r in self._by_engine.values() if r.port}
