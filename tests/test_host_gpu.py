"""Real-hardware host tests (DESIGN.md 20 "Host tests").

These exercise the real NVML inventory provider on an actual NVIDIA host — they
cannot run in CI without a GPU, so they are skipped unless MERIDIAN_GPU_HOST is
set. On a GPU host, run: `MERIDIAN_GPU_HOST=1 pytest tests/test_host_gpu.py`.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("MERIDIAN_GPU_HOST"),
    reason="requires an NVIDIA GPU host; set MERIDIAN_GPU_HOST=1 to enable",
)


def test_nvml_reports_at_least_one_gpu():
    from meridian_node.inventory import InventoryUnavailable, NvmlProvider, inventory_digest

    try:
        gpus = NvmlProvider().gpus()
    except InventoryUnavailable as e:
        pytest.fail(f"NVML unavailable on a host that claims GPUs: {e}")
    assert gpus, "expected at least one GPU"
    for g in gpus:
        assert g["device_id"]
        assert g["memory_total_bytes"] > 0
        assert 0 <= g["memory_free_bytes"] <= g["memory_total_bytes"]
    # The digest is stable for identical inventory.
    assert inventory_digest(gpus) == inventory_digest(gpus)


def test_nvml_digest_changes_with_inventory():
    from meridian_node.inventory import inventory_digest

    a = [{"device_id": "GPU-0", "memory_free_bytes": 1}]
    b = [{"device_id": "GPU-0", "memory_free_bytes": 2}]
    assert inventory_digest(a) != inventory_digest(b)
