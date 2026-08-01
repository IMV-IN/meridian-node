"""Phase 0 exit criteria: enroll, heartbeat, persist state, recover on restart,
and refuse a generation regression (DESIGN.md 21 Phase 0, 17.5)."""

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


def test_enroll_auto_approve_and_heartbeat(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent = make_agent(tmp_path, fake, clock)

    node_id = agent.ensure_enrolled()
    assert node_id.startswith("node_")
    agent.establish_session()
    resp = agent.heartbeat_once()

    assert resp["desired_generation"] == 0
    assert agent.state.last_sequence() == 1
    assert agent.state.load_identity()[0] == node_id


def test_pending_enrollment_requires_operator_approval(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent = make_agent(tmp_path, fake, clock, token="manual")

    # Never approved: enrollment does not yield an identity.
    with pytest.raises(ControlError) as exc:
        agent.ensure_enrolled(max_polls=2)
    assert exc.value.code == "PENDING_APPROVAL"
    assert agent.state.load_identity() is None


def test_pending_claim_approved_then_issues_cert(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent = make_agent(tmp_path, fake, clock, token="manual")

    # Drive enroll/approve/get_claim explicitly to exercise the gating.
    resp = fake.presenting("manual").enroll(
        {"node_public_key": agent.identity.public_key_b64url()}
    )
    assert resp["status"] == "pending"
    claim_id = resp["claim_id"]

    pending = fake.get_claim(claim_id, agent.identity.sign_b64url)
    assert pending["status"] == "pending"

    fake.approve_claim(claim_id)
    approved = fake.get_claim(claim_id, agent.identity.sign_b64url)
    assert approved["status"] == "approved"
    assert approved["node_id"].startswith("node_")


def test_restart_recovers_identity_and_generation(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)

    agent_a = make_agent(tmp_path, fake, clock)
    node_id = agent_a.ensure_enrolled()
    agent_a.establish_session()
    agent_a.heartbeat_once()

    # Control plane advertises generation 5; the agent persists it.
    fake.nodes[node_id].desired_generation = 5
    agent_a.heartbeat_once()
    assert agent_a.state.accepted_generation() == 5
    agent_a.close()

    # Restart: fresh process, same state dir. No re-enrollment.
    clock.advance(31)  # let the prior lease expire so takeover is allowed
    agent_b = make_agent(tmp_path, fake, clock)
    assert agent_b.node_id == node_id
    assert agent_b.state.accepted_generation() == 5
    assert agent_b.state.last_sequence() == 2

    agent_b.establish_session()
    resp = agent_b.heartbeat_once()
    assert resp["accepted_sequence"] == 3  # sequence continues monotonically


def test_generation_never_regresses(tmp_path, clock_factory):
    clock = clock_factory()
    fake = make_fake(clock)
    agent = make_agent(tmp_path, fake, clock)
    node_id = agent.ensure_enrolled()
    agent.establish_session()

    fake.nodes[node_id].desired_generation = 5
    agent.heartbeat_once()

    # A restored control plane advertising an older generation is refused.
    fake.nodes[node_id].desired_generation = 3
    with pytest.raises(ValueError, match="generation regression"):
        agent.heartbeat_once()
