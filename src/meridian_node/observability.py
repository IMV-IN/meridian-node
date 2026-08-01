"""Metrics with bounded labels (DESIGN.md 19).

A tiny dependency-free registry that renders Prometheus text. Labels are bounded
by construction: only the metrics and label sets declared here exist, so raw
node/engine IDs can never become unbounded label values. If prometheus_client is
later adopted, this facade is the single place to swap.
"""

from __future__ import annotations

from collections import defaultdict

# Declared metric names (DESIGN.md 19). Recorded via inc/set below.
COUNTERS = {
    "meridian_node_heartbeat_total": ("result",),
    "meridian_node_reconcile_total": ("result",),
    "meridian_node_assignment_restarts_total": ("engine_kind",),
    "meridian_node_artifact_bytes_total": ("operation", "result"),
    "meridian_node_artifact_verification_total": ("result",),
}
GAUGES = {
    "meridian_node_lease_seconds": ("state",),
    "meridian_node_assignment_info": ("engine_kind", "phase"),
    "meridian_node_gpu_memory_bytes": ("device_class", "state"),
}


class Metrics:
    def __init__(self):
        self._counters: dict[tuple, float] = defaultdict(float)
        self._gauges: dict[tuple, float] = {}

    def _check(self, table: dict, name: str, labels: dict) -> tuple:
        if name not in table:
            raise KeyError(f"undeclared metric {name}")
        expected = set(table[name])
        if set(labels) != expected:
            raise ValueError(f"{name} requires labels {expected}, got {set(labels)}")
        return (name, tuple(sorted(labels.items())))

    def inc(self, name: str, amount: float = 1.0, **labels) -> None:
        self._counters[self._check(COUNTERS, name, labels)] += amount

    def set(self, name: str, value: float, **labels) -> None:
        self._gauges[self._check(GAUGES, name, labels)] = value

    def render(self) -> str:
        lines = []
        for (name, labels), value in sorted(self._counters.items()):
            lines.append(f"{name}{{{_fmt(labels)}}} {value}")
        for (name, labels), value in sorted(self._gauges.items()):
            lines.append(f"{name}{{{_fmt(labels)}}} {value}")
        return "\n".join(lines) + ("\n" if lines else "")


def _fmt(labels: tuple) -> str:
    return ",".join(f'{k}="{v}"' for k, v in labels)
