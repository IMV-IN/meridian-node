"""Live end-to-end over real HTTP: the agent's real HttpTransport drives the
dev control-plane server through the full managed loop. This exercises the real
network path (sockets, headers, error envelope, the claim possession handshake)
that the MockTransport unit tests cannot.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest
from engine_helpers import build_supervisor, make_assignment, make_signed_manifest

from meridian_node.agent import Agent
from meridian_node.config import Config
from meridian_node.devserver import build_server
from meridian_node.http_transport import HttpTransport
from meridian_node.models import DesiredSnapshot
from meridian_node.models import EnginePhase as P
from meridian_node.reconcile import Reconciler
from meridian_node.refcontrol import ControlCore, Token
from meridian_node.runtime import FakeRuntime


@pytest.fixture
def live_server():
    core = ControlCore({"auto": Token(auto_approve=True), "manual": Token()}, lease_ttl_seconds=30)
    server = build_server(core, "127.0.0.1", 0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield core, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()


def test_enroll_session_heartbeat_over_http(live_server, tmp_path):
    core, url = live_server
    transport = HttpTransport(url, enrollment_token="auto")
    cfg = Config(control_plane_url=url, state_dir=tmp_path, mode="observe-only")
    agent = Agent(cfg, transport)

    node_id = agent.ensure_enrolled()
    assert node_id in core.nodes
    agent.establish_session()
    resp = agent.heartbeat_once()
    assert resp["accepted_sequence"] == 1
    assert "lease_expires_at" in resp


def test_pending_approval_handshake_over_http(live_server, tmp_path):
    core, url = live_server
    transport = HttpTransport(url, enrollment_token="manual")
    cfg = Config(control_plane_url=url, state_dir=tmp_path, mode="observe-only")
    agent = Agent(cfg, transport)

    # No approval yet -> enrollment does not complete.
    import pytest as _pytest

    from meridian_node.control import ControlError
    with _pytest.raises(ControlError):
        agent.ensure_enrolled(max_polls=2)

    # Approve the pending claim, then enrollment (with the possession proof) succeeds.
    claim_id = next(iter(core.claims))
    core.approve_claim(claim_id)
    node_id = agent.ensure_enrolled(max_polls=2)
    assert node_id in core.nodes


def test_full_managed_loop_over_http(live_server, tmp_path):
    core, url = live_server
    transport = HttpTransport(url, enrollment_token="auto")
    cfg = Config(control_plane_url=url, state_dir=tmp_path, mode="managed-host")
    agent = Agent(cfg, transport)
    node_id = agent.ensure_enrolled()
    agent.establish_session()

    runtime = FakeRuntime()
    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"w"})
    now = lambda: datetime.now(timezone.utc)  # noqa: E731
    sup = build_supervisor(tmp_path, runtime, manifest, keys, tmp_path / "src", now, node_id=node_id)
    agent.attach_reconciler(Reconciler(node_id, sup, agent.state))

    a = make_assignment(engine_id="engine_1", generation=1, manifest_digest=digest, revision=revision)
    a.node_id = node_id
    snapshot = DesiredSnapshot(
        node_id=node_id, generation=1, snapshot_hash="sha256:" + "0" * 64, assignments=[a]
    ).model_dump(mode="json")
    # Publish desired state via the dev admin endpoint path (direct core call here).
    core.set_desired(node_id, snapshot)

    agent.tick()  # heartbeat -> fetch desired over HTTP -> reconcile -> observe over HTTP
    assert sup.state("engine_1").phase == P.WARMING
    assert core.observations[node_id]["engines"][0]["phase"] == "Warming"

    runtime.mark_ready(sup.state("engine_1").handle.runtime_id, "test/model")
    agent.tick()
    assert sup.state("engine_1").phase == P.READY
