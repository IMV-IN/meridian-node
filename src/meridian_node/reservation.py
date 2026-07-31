"""Local reservation transaction (DESIGN.md 17.4).

Before launch the agent atomically reserves device IDs, a GPU-memory budget,
host RAM/CPU are represented as a single budget here, artifact disk, and a port.
Observed free VRAM is only a placement hint; central approval is necessary but
not sufficient. Partial reservations roll back as one unit. A reservation is
held until the process is confirmed terminated.
"""

from __future__ import annotations

from dataclasses import dataclass, field


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
    _by_engine: dict[str, _Reservation] = field(default_factory=dict)

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

        # All checks passed; commit as one unit.
        self._by_engine[engine_id] = _Reservation(engine_id, tuple(device_ids), gpu_memory_bytes, port)

    def release(self, engine_id: str) -> None:
        self._by_engine.pop(engine_id, None)

    def holds(self, engine_id: str) -> bool:
        return engine_id in self._by_engine
