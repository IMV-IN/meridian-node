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


class InventoryUnavailable(Exception):
    """NVML could not be initialized. The caller reports the device condition as
    Unknown and does not fabricate capacity (DESIGN.md 8.2, 16.5)."""


class NvmlProvider:
    """Read-only NVIDIA inventory via NVML (Phase 1). MIG devices are reported as
    children of their physical GPU and are inventory-only until reservation
    tests pass (DESIGN.md 6.5). Requires `pynvml`; absence fails loud, not open.
    """

    def __init__(self):
        try:
            import pynvml  # type: ignore[import-not-found]  # noqa: F401
        except ImportError as e:
            raise InventoryUnavailable("pynvml is not installed") from e
        self._pynvml = pynvml
        try:
            pynvml.nvmlInit()
        except Exception as e:  # NVML present but no driver/device
            raise InventoryUnavailable(f"NVML init failed: {e}") from e

    def gpus(self) -> list[dict]:
        import time

        nv = self._pynvml
        observed_at = time.time()
        out: list[dict] = []
        count = nv.nvmlDeviceGetCount()
        for i in range(count):
            h = nv.nvmlDeviceGetHandleByIndex(i)
            uuid = nv.nvmlDeviceGetUUID(h)
            mem = nv.nvmlDeviceGetMemoryInfo(h)
            out.append({
                "device_id": _text(uuid),
                "parent_device_id": None,
                "kind": "physical",
                "vendor": "NVIDIA",
                "model": _text(nv.nvmlDeviceGetName(h)),
                "memory_total_bytes": int(mem.total),
                "memory_free_bytes": int(mem.free),
                "memory_used_bytes": int(mem.used),
                "health": "healthy",
                "observed_at": observed_at,
            })
            out.extend(self._mig_children(h, _text(uuid), observed_at))
        return out

    def _mig_children(self, parent_handle, parent_uuid: str, observed_at: float) -> list[dict]:
        nv = self._pynvml
        children: list[dict] = []
        try:
            mig_count = nv.nvmlDeviceGetMaxMigDeviceCount(parent_handle)
        except Exception:
            return children
        for j in range(mig_count):
            try:
                mh = nv.nvmlDeviceGetMigDeviceHandleByIndex(parent_handle, j)
                uuid = _text(nv.nvmlDeviceGetUUID(mh))
            except Exception:
                continue
            children.append({
                "device_id": uuid,
                "parent_device_id": parent_uuid,
                "kind": "mig",
                "vendor": "NVIDIA",
                "health": "healthy",
                "observed_at": observed_at,
            })
        return children


def _text(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)
