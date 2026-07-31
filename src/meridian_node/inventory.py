"""GPU/host inventory providers.

Phase 0 ships the provider interface and a deterministic fake so the agent and
control protocol are testable without NVIDIA hardware. The real NVML provider
(read-only) lands in Phase 1. Reported capacity always carries an observation
timestamp; stale inventory is treated as unknown, never as free capacity
(DESIGN.md 8.2).
"""

from __future__ import annotations

import hashlib
import json
from typing import Protocol


class InventoryProvider(Protocol):
    def gpus(self) -> list[dict]:
        ...


class FakeNvidiaProvider:
    """Static GPU inventory for tests and observe-only demos."""

    def __init__(self, devices: list[dict] | None = None):
        self._devices = devices if devices is not None else [
            {
                "device_id": "GPU-fake-0",
                "parent_device_id": None,
                "kind": "physical",
                "vendor": "NVIDIA",
                "model": "Fake RTX",
                "memory_total_bytes": 8 * 1024**3,
                "memory_free_bytes": 8 * 1024**3,
                "health": "healthy",
            }
        ]

    def gpus(self) -> list[dict]:
        return [dict(d) for d in self._devices]


def inventory_digest(gpus: list[dict]) -> str:
    """Stable content digest of the reported inventory."""
    canonical = json.dumps(gpus, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(canonical).hexdigest()
