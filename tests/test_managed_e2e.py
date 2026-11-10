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


def test_fresh_node_reports_capacity_before_any_assignment_and_then_runs_a_job(tmp_path, clock_factory):
    """Placement and marketplace listings need a node's capacity before anything
    has been published for it, and the first thing published may be a job."""
    from meridian_node.jobs import JobPolicy, JobSupervisor
    from meridian_node.models import JobAssignment

    clock = clock_factory()
    fake = FakeControlPlane({"auto": Token(auto_approve=True)}, now=clock, lease_ttl_seconds=30)
    agent = Agent(Config(control_plane_url="fake://cp", state_dir=tmp_path, mode="managed-host"),
                  fake.presenting("auto"), now=clock)
    node_id = agent.ensure_enrolled()
    agent.establish_session()
    runtime = FakeRuntime()
    manifest, _, keys, _ = make_signed_manifest(tmp_path / "src", {"model.bin": b"w"})
    sup = build_supervisor(tmp_path, runtime, manifest, keys, tmp_path / "src", clock, node_id=node_id)
    sup.reservations.job_port_range = range(9000, 9010)
    jobs = JobSupervisor(node_id, runtime, sup.journal, sup.reservations,
                         JobPolicy(enabled=True, allow_any_image=True), "10.0.0.5", now=clock)
    agent.attach_reconciler(Reconciler(node_id, sup, agent.state, now=clock, jobs=jobs))

    agent.tick()  # nothing published yet
    cap = fake.observations[node_id]["capacity"]
    device = cap["free_devices"][0]
    assert cap["jobs_enabled"] is True and cap["job_port_range"] == [9000, 9010]
    assert fake.observations[node_id]["observed_generation"] == 0

    job = JobAssignment(job_id="job_1", image="docker.io/library/alpine@sha256:" + "c" * 64, device_ids=[device])
    snap = DesiredSnapshot(node_id=node_id, generation=1, snapshot_hash="sha256:" + "0" * 64, jobs=[job])
    fake.set_desired(node_id, snap.model_dump(mode="json"))
    agent.tick()
    obs = fake.observations[node_id]
    assert obs["jobs"][0]["phase"] == "Running" and device not in obs["capacity"]["free_devices"]
