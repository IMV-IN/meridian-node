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
