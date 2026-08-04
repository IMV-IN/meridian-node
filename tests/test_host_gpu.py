"""Real-hardware host tests (DESIGN.md 20 "Host tests").

These exercise the real NVML inventory provider on an actual NVIDIA host — they
cannot run in CI without a GPU, so they are skipped unless MERIDIAN_GPU_HOST is
set. On a GPU host: `MERIDIAN_GPU_HOST=1 pip install -e '.[http,gpu,dev]' &&
pytest tests/test_host_gpu.py`.

Executed and passing on an NVIDIA GeForce RTX 4060 (driver 580, 8 GB).
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("MERIDIAN_GPU_HOST"),
    reason="requires an NVIDIA GPU host; set MERIDIAN_GPU_HOST=1 to enable",
)


def _real_gpus():
    from meridian_node.inventory import InventoryUnavailable, NvmlProvider

    try:
        return NvmlProvider().gpus()
    except InventoryUnavailable as e:
        pytest.fail(f"NVML unavailable on a host that claims GPUs: {e}")


# --- inventory provider -------------------------------------------------
def test_nvml_reports_real_devices():
    from meridian_node.inventory import inventory_digest

    gpus = _real_gpus()
    assert gpus, "expected at least one GPU"
    physical = [g for g in gpus if g["kind"] == "physical"]
    assert physical, "expected at least one physical GPU"
    for g in physical:
        assert g["device_id"].startswith("GPU-")  # NVML UUID
        assert g["vendor"] == "NVIDIA"
        assert "NVIDIA" in g["model"]
        assert g["memory_total_bytes"] > 0
        assert 0 <= g["memory_free_bytes"] <= g["memory_total_bytes"]
    # MIG children (if any) reference a real physical parent.
    parents = {g["device_id"] for g in physical}
    for child in (g for g in gpus if g["kind"] == "mig"):
        assert child["parent_device_id"] in parents
    # Digest is stable for identical inventory.
    assert inventory_digest(gpus) == inventory_digest(gpus)


def test_nvml_digest_changes_with_inventory():
    from meridian_node.inventory import inventory_digest

    a = [{"device_id": "GPU-0", "memory_free_bytes": 1}]
    b = [{"device_id": "GPU-0", "memory_free_bytes": 2}]
    assert inventory_digest(a) != inventory_digest(b)


# --- real inventory through a real agent -> HTTP -> control loop --------
def test_real_gpu_inventory_flows_through_agent_heartbeat(tmp_path):
    """The real NVML inventory serializes, transmits over HTTP, and is accepted by
    the control plane in an observe-only heartbeat."""
    from meridian_node.agent import Agent
    from meridian_node.config import Config
    from meridian_node.devserver import build_server
    from meridian_node.http_transport import HttpTransport
    from meridian_node.inventory import NvmlProvider, inventory_digest
    from meridian_node.refcontrol import ControlCore, Token

    core = ControlCore({"auto": Token(auto_approve=True)}, lease_ttl_seconds=30)
    server = build_server(core, "127.0.0.1", 0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{port}"
        cfg = Config(control_plane_url=url, state_dir=tmp_path, mode="observe-only")
        agent = Agent(cfg, HttpTransport(url, enrollment_token="auto"),
                      inventory=NvmlProvider(), now=lambda: datetime.now(timezone.utc))
        agent.ensure_enrolled()
        agent.establish_session()
        resp = agent.heartbeat_once()
        assert resp["accepted_sequence"] == 1  # real GPU payload accepted over HTTP

        gpus = agent.inventory.gpus()
        assert any("NVIDIA" in g.get("model", "") for g in gpus if g["kind"] == "physical")
        assert inventory_digest(gpus).startswith("sha256:")
    finally:
        server.shutdown()
