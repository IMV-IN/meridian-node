"""Reconciliation: snapshot application, deletion via drain, generation
monotonicity, and observed-state emission (DESIGN.md 11)."""

from __future__ import annotations

import pytest
from engine_helpers import build_supervisor, make_assignment, make_signed_manifest

from meridian_node.models import DesiredSnapshot
from meridian_node.models import EnginePhase as P
from meridian_node.reconcile import Reconciler
from meridian_node.runtime import FakeRuntime
from meridian_node.state import StateStore


def _snapshot(generation, assignments):
    return DesiredSnapshot(
        node_id="node_test",
        generation=generation,
        snapshot_hash="sha256:" + "0" * 64,
        assignments=assignments,
    )


def _wire(tmp_path, clock):
    runtime = FakeRuntime()
    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"weights"})
    sup = build_supervisor(tmp_path, runtime, manifest, keys, tmp_path / "src", clock)
    state = StateStore(tmp_path / "state.db")
    rec = Reconciler("node_test", sup, state, now=clock)
    a = make_assignment(manifest_digest=digest, revision=revision)
    return runtime, sup, rec, a


def test_apply_snapshot_reaches_ready(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, rec, a = _wire(tmp_path, clock)

    obs = rec.apply(_snapshot(1, [a]))
    assert obs["engines"][0]["phase"] == P.WARMING.value
    handle = sup.state(a.engine_id).handle
    runtime.mark_ready(handle.runtime_id, "test/model")

    obs = rec.apply(_snapshot(1, [a]))
    assert obs["engines"][0]["phase"] == P.READY.value
    assert obs["engines"][0]["endpoint"].endswith(":8000")


def test_generation_cannot_regress(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, rec, a = _wire(tmp_path, clock)
    rec.apply(_snapshot(5, [a]))
    with pytest.raises(ValueError, match="lower desired generation"):
        rec.apply(_snapshot(3, [a]))


def test_deletion_drains_then_stops(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, rec, a = _wire(tmp_path, clock)
    rec.apply(_snapshot(1, [a]))
    handle = sup.state(a.engine_id).handle
    runtime.mark_ready(handle.runtime_id, "test/model")
    rec.apply(_snapshot(1, [a]))
    assert sup.state(a.engine_id).phase == P.READY

    # Next generation omits the assignment -> deletion. Not yet authorized to stop.
    rec.apply(_snapshot(2, []), stop_authorized=set())
    assert sup.state(a.engine_id).phase == P.DRAINING
    assert runtime.inspect(handle.runtime_id).alive

    # Central authorizes the stop (all replicas withdrawn).
    rec.apply(_snapshot(3, []), stop_authorized={a.engine_id})
    assert sup.state(a.engine_id).phase == P.STOPPED
    assert not runtime.inspect(handle.runtime_id).alive


def test_snapshot_for_wrong_node_rejected(tmp_path, clock_factory):
    clock = clock_factory()
    runtime, sup, rec, a = _wire(tmp_path, clock)
    snap = _snapshot(1, [a])
    snap.node_id = "someone-else"
    with pytest.raises(ValueError, match="applied on"):
        rec.apply(snap)
