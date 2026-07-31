"""Supervisor lifecycle: start->ready, bad digest, crash/backoff/quarantine,
drain safety, adoption, and orphan handling (DESIGN.md 9.2, 11, 14, 16)."""

from __future__ import annotations

from engine_helpers import build_supervisor, make_assignment, make_signed_manifest

from meridian_node.models import EnginePhase as P
from meridian_node.runtime import FakeRuntime


def _setup(tmp_path, clock):
    runtime = FakeRuntime()
    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"weights"})
    sup = build_supervisor(tmp_path, runtime, manifest, keys, tmp_path / "src", clock)
    a = make_assignment(manifest_digest=digest, revision=revision)
    return runtime, sup, a


def _run_to_ready(runtime, sup, a):
    sup.reconcile_engine(a)  # Pending -> ... -> Warming (launch)
    handle = sup.state(a.engine_id).handle
    runtime.mark_ready(handle.runtime_id, "test/model")
    sup.reconcile_engine(a)  # Warming -> Ready
    return handle


def test_start_to_ready(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, a = _setup(tmp_path, clock)
    _run_to_ready(runtime, sup, a)
    assert sup.state(a.engine_id).phase == P.READY


def test_bad_manifest_digest_fails_closed(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, a = _setup(tmp_path, clock)
    a.artifact.manifest_digest = "sha256:" + "9" * 64  # not the signed digest
    sup.reconcile_engine(a)
    st = sup.state(a.engine_id)
    assert st.phase == P.FAILED
    assert st.reason == "ArtifactVerificationFailed"


def test_crash_triggers_backoff_then_quarantine(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, a = _setup(tmp_path, clock)  # max_attempts=2, backoff=10
    handle = _run_to_ready(runtime, sup, a)

    runtime.crash(handle.runtime_id)
    sup.reconcile_engine(a)
    assert sup.state(a.engine_id).phase == P.FAILED
    assert sup.state(a.engine_id).restart_count == 1

    # Within backoff window: no restart yet.
    sup.reconcile_engine(a)
    assert sup.state(a.engine_id).phase == P.FAILED

    # After backoff: restarts, then crash again, and again -> quarantine.
    clock.advance(11)
    sup.reconcile_engine(a)  # Pending -> Warming (relaunch)
    h2 = sup.state(a.engine_id).handle
    runtime.crash(h2.runtime_id)
    sup.reconcile_engine(a)  # Failed, restart_count 2
    clock.advance(30)
    sup.reconcile_engine(a)  # relaunch
    h3 = sup.state(a.engine_id).handle
    runtime.crash(h3.runtime_id)
    sup.reconcile_engine(a)  # restart_count 3 > max_attempts 2 -> Quarantined
    assert sup.state(a.engine_id).phase == P.QUARANTINED

    sup.operator_reset(a.engine_id)
    assert sup.state(a.engine_id).phase == P.PENDING


def test_drain_does_not_stop_until_authorized(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, a = _setup(tmp_path, clock)
    handle = _run_to_ready(runtime, sup, a)

    a.desired_phase = "Draining"
    sup.reconcile_engine(a, can_stop=False)
    assert sup.state(a.engine_id).phase == P.DRAINING
    assert runtime.inspect(handle.runtime_id).alive  # still running

    sup.reconcile_engine(a, can_stop=True)
    assert sup.state(a.engine_id).phase == P.STOPPED
    assert not runtime.inspect(handle.runtime_id).alive


def test_adoption_of_matching_process(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, a = _setup(tmp_path, clock)
    _run_to_ready(runtime, sup, a)

    # Simulate a fresh supervisor (agent restart) over the same runtime.
    sup2 = build_supervisor(tmp_path, runtime, *_manifest_keys(tmp_path), tmp_path / "src", clock)
    sup2.discover({a.engine_id: a})
    st = sup2.state(a.engine_id)
    assert st.handle is not None  # adopted
    assert st.phase == P.WARMING  # re-verifies readiness before Ready


def test_orphan_process_not_adopted(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, a = _setup(tmp_path, clock)
    runtime.orphan(image_digest="sha256:" + "f" * 64, port=9999, marker="someone-else:x:y")
    sup.discover({a.engine_id: a})
    assert len(sup.orphans) == 1
    assert sup.orphans[0].port == 9999


def _manifest_keys(tmp_path):
    # Rebuild manifest+keys deterministically from the same source files.
    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"weights"})
    return manifest, keys
