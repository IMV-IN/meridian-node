"""Fencing and replay rejection (DESIGN.md 8.1, 16.7, 23).

These are the safety tests that gate any engine control: stale sessions and
replayed messages must never mutate current state.
"""

from __future__ import annotations

import pytest
from fake_control_plane import FakeControlPlane, Token

from meridian_node.agent import Agent
from meridian_node.config import Config
from meridian_node.control import ControlError


def make_fake(clock):
    return FakeControlPlane({"auto": Token(auto_approve=True), "manual": Token()}, now=clock, lease_ttl_seconds=30)


def make_agent(tmp_path, fake, clock, token="auto"):
    cfg = Config(control_plane_url="fake://cp", state_dir=tmp_path, mode="managed-host")
    return Agent(cfg, fake.presenting(token), now=clock)


def test_replayed_enrollment_token_rejected(clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    fake.presenting("auto").enroll({"node_public_key": "AAAA"})
    with pytest.raises(ControlError) as exc:
        fake.presenting("auto").enroll({"node_public_key": "AAAA"})
    assert exc.value.code == "INVALID_CREDENTIAL"


def test_heartbeat_sequence_must_strictly_increase(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent = make_agent(tmp_path, fake, clock)
    node_id = agent.ensure_enrolled()
    agent.establish_session()
    agent.heartbeat_once()  # sequence 1 accepted

    epoch = agent.fencing_epoch
    with pytest.raises(ControlError) as exc:
        fake.heartbeat(node_id, _hb(agent, sequence=1, epoch=epoch))
    assert exc.value.code == "CONFLICT"
    with pytest.raises(ControlError):
        fake.heartbeat(node_id, _hb(agent, sequence=0, epoch=epoch))
    # A strictly greater sequence is accepted.
    fake.heartbeat(node_id, _hb(agent, sequence=2, epoch=epoch))


def test_stale_session_cannot_heartbeat(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent_a = make_agent(tmp_path, fake, clock)
    agent_a.ensure_enrolled()
    agent_a.establish_session()
    agent_a.heartbeat_once()

    # A second agent process takes over after the lease expires.
    clock.advance(31)
    agent_b = make_agent(tmp_path, fake, clock)
    agent_b.establish_session()
    assert agent_b.fencing_epoch > agent_a.fencing_epoch

    # The old session's heartbeat is now fenced out.
    with pytest.raises(ControlError) as exc:
        agent_a.heartbeat_once()
    assert exc.value.code == "STALE_AGENT_SESSION"


def test_session_blocked_while_prior_lease_valid(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent_a = make_agent(tmp_path, fake, clock)
    agent_a.ensure_enrolled()
    agent_a.establish_session()
    agent_a.heartbeat_once()

    agent_b = make_agent(tmp_path, fake, clock)
    with pytest.raises(ControlError) as exc:
        agent_b.establish_session()
    assert exc.value.code == "CONFLICT"

    # After lease expiry the takeover is permitted.
    clock.advance(31)
    agent_b.establish_session()
    assert agent_b.fencing_epoch > agent_a.fencing_epoch


def _hb(agent, sequence, epoch):
    return {
        "host_boot_id": agent.host_boot_id,
        "agent_session_id": agent.session_id,
        "fencing_epoch": epoch,
        "sequence": sequence,
        "sent_at": "2026-08-01T12:00:00Z",
        "agent": {"version": "0.1.0", "protocol": "v1", "mode": "managed-host"},
    }


def test_helper_learns_the_epoch_when_a_session_is_established(tmp_path, clock_factory):
    """In production nothing raised the helper's fence, so STALE_EPOCH never fired.
    Establishing a session (and attaching the reconciler) now tells the helper, a
    request carrying an older epoch is refused, and the fence survives a helper
    restart because it is journaled."""
    from engine_helpers import build_supervisor, make_signed_manifest

    from meridian_node.helper import FakeContainerBackend, HelperError, HelperPolicy, HelperServer
    from meridian_node.journal import Journal
    from meridian_node.reconcile import Reconciler
    from meridian_node.runtime import HelperRuntime

    clock = clock_factory()
    fake = make_fake(clock)
    policy = HelperPolicy(frozenset(), frozenset(), 8000, 8100, ())
    server = HelperServer(FakeContainerBackend(), Journal(tmp_path / "helper.db"), policy)

    agent_a = make_agent(tmp_path / "a", fake, clock)
    node_id = agent_a.ensure_enrolled()
    agent_a.establish_session()
    runtime = HelperRuntime(server.handle, session_id=agent_a.session_id, fencing_epoch=0)
    manifest, _, keys, _ = make_signed_manifest(tmp_path / "src", {"m": b"w"})
    sup = build_supervisor(tmp_path / "a", runtime, manifest, keys, tmp_path / "src", clock, node_id=node_id)
    agent_a.attach_reconciler(Reconciler(node_id, sup, agent_a.state, now=clock))
    first = agent_a.fencing_epoch
    assert server.highest_epoch == first and runtime.fencing_epoch == first and sup.fencing_epoch == first

    # The same agent re-establishes (for example after STALE_AGENT_SESSION): the helper follows.
    clock.advance(31)
    agent_a.establish_session()
    assert agent_a.fencing_epoch > first and server.highest_epoch == agent_a.fencing_epoch
    assert runtime.list_all() == []  # the current epoch is accepted

    def stale(op):
        return {"operation_id": op, "agent_session_id": "old", "nonce": op, "fencing_epoch": first,
                "type": "stop", "payload": {"runtime_id": "ctr-1"}}

    with pytest.raises(HelperError) as exc:
        server.handle(stale("o1"))
    assert exc.value.code == "STALE_EPOCH"
    with pytest.raises(HelperError) as exc:
        HelperRuntime(server.handle, "old", first).refresh_epoch(first)  # never lowered
    assert exc.value.code == "STALE_EPOCH"

    restarted = HelperServer(FakeContainerBackend(), Journal(tmp_path / "helper.db"), policy)
    assert restarted.highest_epoch == agent_a.fencing_epoch
    with pytest.raises(HelperError) as exc:
        restarted.handle(stale("o2"))
    assert exc.value.code == "STALE_EPOCH"


def test_helper_refuses_a_malformed_epoch(tmp_path):
    from meridian_node.helper import FakeContainerBackend, HelperError, HelperPolicy, HelperServer
    from meridian_node.journal import Journal

    server = HelperServer(FakeContainerBackend(), Journal(tmp_path / "h.db"), HelperPolicy(
        frozenset(), frozenset(), 8000, 8100, ()))
    for bad in ("7", -1, True, 1.5, None):
        with pytest.raises(HelperError) as exc:
            server.handle({"operation_id": "o", "agent_session_id": "s", "nonce": "n", "fencing_epoch": bad,
                           "type": "list", "payload": {}})
        assert exc.value.code == "INVALID_REQUEST"
