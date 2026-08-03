"""End-to-end managed-host loop against the fake control plane: enroll, publish
desired state, reconcile an engine to Ready, then delete it through drain/stop.
This is the integration that ties the whole agent together (DESIGN.md 20)."""

from __future__ import annotations

from engine_helpers import build_supervisor, make_assignment, make_signed_manifest
from fake_control_plane import FakeControlPlane, Token

from meridian_node.agent import Agent
from meridian_node.config import Config
from meridian_node.models import DesiredSnapshot
from meridian_node.models import EnginePhase as P
from meridian_node.reconcile import Reconciler
from meridian_node.runtime import FakeRuntime


def _snapshot_dict(node_id, generation, assignments):
    return DesiredSnapshot(
        node_id=node_id,
        generation=generation,
        snapshot_hash="sha256:" + "0" * 64,
        assignments=assignments,
    ).model_dump(mode="json")


def test_full_managed_lifecycle(tmp_path, clock_factory):
    clock = clock_factory()
    fake = FakeControlPlane({"auto": Token(auto_approve=True)}, now=clock, lease_ttl_seconds=30)

    cfg = Config(control_plane_url="fake://cp", state_dir=tmp_path, mode="managed-host")
    agent = Agent(cfg, fake.presenting("auto"), now=clock)
    node_id = agent.ensure_enrolled()
    agent.establish_session()

    runtime = FakeRuntime()
    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"weights"})
    sup = build_supervisor(tmp_path, runtime, manifest, keys, tmp_path / "src", clock, node_id=node_id)
    reconciler = Reconciler(node_id, sup, agent.state, now=clock)
    agent.attach_reconciler(reconciler)

    a = make_assignment(engine_id="engine_1", generation=1, manifest_digest=digest, revision=revision)
    a.node_id = node_id
    fake.set_desired(node_id, _snapshot_dict(node_id, 1, [a]))

    # Tick 1: fetch desired, launch, engine is Warming.
    agent.tick()
    assert sup.state("engine_1").phase == P.WARMING
    assert fake.observations[node_id]["engines"][0]["phase"] == "Warming"

    # Engine becomes ready; next tick promotes to Ready and reports an endpoint.
    runtime.mark_ready(sup.state("engine_1").handle.runtime_id, "test/model")
    agent.tick()
    assert sup.state("engine_1").phase == P.READY
    assert fake.observations[node_id]["engines"][0]["endpoint"].endswith(":8000")

    # Delete the assignment (generation 2). Drains but does not stop yet.
    fake.set_desired(node_id, _snapshot_dict(node_id, 2, []))
    agent.tick()
    assert sup.state("engine_1").phase == P.DRAINING

    # Central authorizes the stop; next tick stops the engine.
    fake.stop_authorized[node_id] = {"engine_1"}
    fake.set_desired(node_id, _snapshot_dict(node_id, 3, []))
    agent.tick()
    assert sup.state("engine_1").phase == P.STOPPED


def test_observe_only_cannot_attach_reconciler(tmp_path, clock_factory):
    clock = clock_factory()
    fake = FakeControlPlane({"auto": Token(auto_approve=True)}, now=clock)
    cfg = Config(control_plane_url="fake://cp", state_dir=tmp_path, mode="observe-only")
    agent = Agent(cfg, fake.presenting("auto"), now=clock)
    import pytest
    with pytest.raises(ValueError, match="engine-launching mode"):
        agent.attach_reconciler(object())
