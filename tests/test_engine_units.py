"""Unit tests for the engine-layer building blocks: state machine, drivers,
reservation ledger, journal, and artifact trust chain."""

from __future__ import annotations

import pytest
from engine_helpers import LocalFetcher, make_assignment, make_signed_manifest

from meridian_node.artifacts import ArtifactStore, VerificationError, verify_manifest
from meridian_node.drivers import DriverError, get_driver
from meridian_node.journal import COMMITTED, Journal, JournalConflict
from meridian_node.models import EnginePhase as P
from meridian_node.reservation import ReservationError, ReservationLedger
from meridian_node.statemachine import IllegalTransition, assert_transition, can_transition


# --- state machine ------------------------------------------------------
def test_legal_and_illegal_transitions():
    assert can_transition(P.WARMING, P.READY)
    assert can_transition(P.READY, P.DRAINING)
    assert not can_transition(P.READY, P.PENDING)
    with pytest.raises(IllegalTransition):
        assert_transition(P.STOPPED, P.READY)


# --- drivers ------------------------------------------------------------
def test_vllm_build_launch_and_param_allowlist():
    driver = get_driver("vllm")
    a = make_assignment(manifest_digest="sha256:" + "1" * 64)
    gpus = [{"device_id": "GPU-fake-0"}]
    driver.validate(a, gpus)
    req = driver.build_launch(a, "/art/path", "node:engine:op")
    assert "--model" in req.args and "/art/path" in req.args
    assert req.ownership_marker == "node:engine:op"


def test_vllm_rejects_unknown_parameter():
    driver = get_driver("vllm")
    a = make_assignment(manifest_digest="sha256:" + "1" * 64)
    a.parameters["danger"] = "rm -rf"
    with pytest.raises(DriverError, match="unknown engine parameters"):
        driver.validate(a, [{"device_id": "GPU-fake-0"}])


def test_driver_rejects_missing_device():
    driver = get_driver("vllm")
    a = make_assignment(manifest_digest="sha256:" + "1" * 64)
    with pytest.raises(DriverError, match="device not present"):
        driver.validate(a, [{"device_id": "some-other-gpu"}])


@pytest.mark.parametrize("kind,params,expect_in_args", [
    ("tgi", {"served_model_name": "m", "num_shard": 2}, ["--model-id", "/art", "--num-shard", "2"]),
    ("sglang", {"served_model_name": "m", "tp_size": 4}, ["--model-path", "/art", "--tp", "4"]),
    ("llamacpp", {"served_model_name": "m", "n_gpu_layers": 99}, ["-m", "/art", "-ngl", "99"]),
    ("lmdeploy", {"served_model_name": "m", "tp": 2}, ["/art", "--tp", "2"]),
])
def test_openai_compatible_drivers(kind, params, expect_in_args):
    driver = get_driver(kind)
    a = make_assignment(manifest_digest="sha256:" + "1" * 64)
    a.engine.kind = kind
    a.parameters = dict(params)
    driver.validate(a, [{"device_id": "GPU-fake-0"}])
    req = driver.build_launch(a, "/art", "node:engine:op")
    for token in expect_in_args:
        assert token in req.args, f"{token} missing from {req.args}"
    assert str(a.resources.port) in req.args
    assert req.env["NVIDIA_VISIBLE_DEVICES"] == "GPU-fake-0"
    assert driver.readiness(a).served_model == "m"

    a.parameters["nope"] = 1
    with pytest.raises(DriverError, match="unknown engine parameters"):
        driver.validate(a, [{"device_id": "GPU-fake-0"}])


def test_openai_compatible_driver_requires_served_model():
    a = make_assignment(manifest_digest="sha256:" + "1" * 64)
    a.engine.kind = "tgi"
    a.parameters = {}
    with pytest.raises(DriverError, match="served_model_name"):
        get_driver("tgi").validate(a, [{"device_id": "GPU-fake-0"}])


def test_ollama_driver_build_launch_and_allowlist():
    driver = get_driver("ollama")
    a = make_assignment(manifest_digest="sha256:" + "1" * 64)
    a.engine.kind = "ollama"
    a.parameters = {"served_model_name": "llama3", "keep_alive": "5m"}
    driver.validate(a, [{"device_id": "GPU-fake-0"}])
    req = driver.build_launch(a, "/models", "node:engine:op")
    assert req.args == ("serve",)
    assert req.env["OLLAMA_HOST"] == "0.0.0.0:8000"
    assert req.env["OLLAMA_MODELS"] == "/models"
    assert req.env["OLLAMA_KEEP_ALIVE"] == "5m"
    assert driver.readiness(a).served_model == "llama3"

    a.parameters["danger"] = "rm -rf"
    with pytest.raises(DriverError, match="unknown engine parameters"):
        driver.validate(a, [{"device_id": "GPU-fake-0"}])


# --- reservation --------------------------------------------------------
def test_reservation_over_commit_and_port_conflict():
    ledger = ReservationLedger(device_capacity={"g0": 8 * 1024**3}, port_range=range(8000, 8100))
    ledger.reserve("e1", ["g0"], 5 * 1024**3, 8000)
    with pytest.raises(ReservationError, match="over-committed"):
        ledger.reserve("e2", ["g0"], 5 * 1024**3, 8001)
    with pytest.raises(ReservationError, match="already reserved"):
        ledger.reserve("e3", ["g0"], 1 * 1024**3, 8000)
    ledger.release("e1")
    ledger.reserve("e2", ["g0"], 5 * 1024**3, 8001)  # now fits


def test_reservation_port_range_enforced():
    ledger = ReservationLedger(device_capacity={"g0": 8 * 1024**3}, port_range=range(8000, 8100))
    with pytest.raises(ReservationError, match="outside configured range"):
        ledger.reserve("e1", ["g0"], 1, 9000)


def test_mig_parent_child_exclusivity():
    g = 1024**3
    cap = {"GPU-0": 40 * g, "GPU-0/mig0": 10 * g, "GPU-0/mig1": 10 * g,
           "GPU-1": 40 * g, "GPU-1/mig0": 10 * g}
    topo = {"GPU-0": None, "GPU-0/mig0": "GPU-0", "GPU-0/mig1": "GPU-0",
            "GPU-1": None, "GPU-1/mig0": "GPU-1"}
    ledger = ReservationLedger(device_capacity=cap, port_range=range(8000, 8100), topology=topo)

    ledger.reserve("e1", ["GPU-0/mig0"], 5 * g, 8000)  # a MIG child
    ledger.reserve("e2", ["GPU-0/mig1"], 5 * g, 8001)  # sibling child — allowed
    # The whole physical GPU is now excluded (its children are reserved).
    with pytest.raises(ReservationError, match="MIG exclusivity"):
        ledger.reserve("e3", ["GPU-0"], 5 * g, 8002)

    # Reverse direction: reserving a whole GPU excludes its children.
    ledger.reserve("e4", ["GPU-1"], 5 * g, 8003)
    with pytest.raises(ReservationError, match="MIG exclusivity"):
        ledger.reserve("e5", ["GPU-1/mig0"], 5 * g, 8004)

    # Physical-only ledger (empty topology) applies no MIG check.
    plain = ReservationLedger(device_capacity={"g0": 8 * g}, port_range=range(8000, 8100))
    plain.reserve("x", ["g0"], 1 * g, 8000)


def test_allocatable_capacity_reflects_reservations():
    ledger = ReservationLedger(device_capacity={"g0": 8 * 1024**3}, port_range=range(8000, 8002))
    assert ledger.allocatable() == {"g0": 8 * 1024**3}
    assert ledger.reserved_ports() == set()
    ledger.reserve("e1", ["g0"], 3 * 1024**3, 8000)
    assert ledger.allocatable() == {"g0": 5 * 1024**3}
    assert ledger.reserved_ports() == {8000}
    ledger.release("e1")
    assert ledger.allocatable() == {"g0": 8 * 1024**3}


# --- journal ------------------------------------------------------------
def test_journal_idempotent_and_digest_conflict(tmp_path):
    j = Journal(tmp_path / "j.db")
    assert j.plan("op1", "sha256:aa", "e1", 1, "launch") == "planned"
    # Same op + digest is a no-op returning the current state.
    j.mark("op1", COMMITTED, result="rt-1")
    assert j.plan("op1", "sha256:aa", "e1", 1, "launch") == COMMITTED
    # Reused id with a different digest is rejected.
    with pytest.raises(JournalConflict):
        j.plan("op1", "sha256:bb", "e1", 1, "launch")
    assert j.inspect("op1").result == "rt-1"


# --- artifacts ----------------------------------------------------------
def test_manifest_signature_and_digest_gate(tmp_path):
    manifest, digest, keys, _ = make_signed_manifest(tmp_path / "src", {"config.json": b"{}"})
    verify_manifest(manifest, keys, digest)  # passes

    with pytest.raises(VerificationError, match="digest mismatch"):
        verify_manifest(manifest, keys, "sha256:" + "0" * 64)

    tampered = {**manifest, "signature": {**manifest["signature"], "key_id": "unknown"}}
    with pytest.raises(VerificationError, match="trust policy"):
        verify_manifest(tampered, keys, digest)


def test_artifact_materialize_verifies_and_promotes(tmp_path):
    manifest, digest, keys, _ = make_signed_manifest(tmp_path / "src", {"model.bin": b"weights"})
    verified = verify_manifest(manifest, keys, digest)
    store = ArtifactStore(tmp_path / "cache")
    path = store.materialize(verified, digest, LocalFetcher(tmp_path / "src"), "local")
    assert (path / "model.bin").read_bytes() == b"weights"
    assert store.has(digest)
    # Idempotent second call returns the same path without refetching.
    assert store.materialize(verified, digest, LocalFetcher(tmp_path / "src"), "local") == path


def test_capacity_report_includes_placement_signals(tmp_path):
    from datetime import datetime, timezone

    from engine_helpers import build_supervisor

    from meridian_node.runtime import FakeRuntime

    manifest, digest, keys, _ = make_signed_manifest(tmp_path / "src", {"m.bin": b"w"})
    sup = build_supervisor(tmp_path, FakeRuntime(), manifest, keys, tmp_path / "src",
                           lambda: datetime.now(timezone.utc))
    sup.nvlink_groups = {"GPU-0": "nv0", "GPU-1": "nv0"}
    rep = sup.capacity_report()
    assert set(rep) >= {"allocatable_vram_bytes", "free_ports", "held_artifacts",
                        "running_engines", "queue_depth", "nvlink_groups"}
    assert rep["running_engines"] == 0
    assert rep["queue_depth"] == 0
    assert rep["held_artifacts"] == []
    assert rep["nvlink_groups"] == {"GPU-0": "nv0", "GPU-1": "nv0"}


def test_capacity_report_aggregates_queue_depth(tmp_path):
    from datetime import datetime, timezone

    from engine_helpers import build_supervisor

    from meridian_node.models import EnginePhase
    from meridian_node.runtime import FakeRuntime

    manifest, digest, keys, _ = make_signed_manifest(tmp_path / "src", {"m.bin": b"w"})
    sup = build_supervisor(tmp_path, FakeRuntime(), manifest, keys, tmp_path / "src",
                           lambda: datetime.now(timezone.utc))
    for eid, depth in (("e1", 3), ("e2", 5)):
        st = sup.state(eid)
        st.phase = EnginePhase.READY
        st.queue_depth = depth
    sup.state("e3").phase = EnginePhase.READY  # queue_depth unknown -> treated as 0
    rep = sup.capacity_report()
    assert rep["running_engines"] == 3
    assert rep["queue_depth"] == 8


def test_artifact_store_held_lists_materialized(tmp_path):
    manifest, digest, keys, _ = make_signed_manifest(tmp_path / "src", {"model.bin": b"w"})
    store = ArtifactStore(tmp_path / "cache")
    assert store.held() == []
    store.materialize(verify_manifest(manifest, keys, digest), digest, LocalFetcher(tmp_path / "src"), "local")
    assert store.held() == [digest]


def test_artifact_rejects_corrupted_file(tmp_path):
    manifest, digest, keys, _ = make_signed_manifest(tmp_path / "src", {"model.bin": b"weights"})
    verified = verify_manifest(manifest, keys, digest)
    (tmp_path / "src" / "model.bin").write_bytes(b"tampered-different-length")
    store = ArtifactStore(tmp_path / "cache")
    with pytest.raises(VerificationError):
        store.materialize(verified, digest, LocalFetcher(tmp_path / "src"), "local")
    assert not store.has(digest)  # nothing promoted


def test_artifact_lru_eviction_respects_pins(tmp_path):
    m1, d1, k1, _ = make_signed_manifest(tmp_path / "s1", {"a.bin": b"x" * 1000}, artifact_id="a1")
    m2, d2, k2, _ = make_signed_manifest(tmp_path / "s2", {"b.bin": b"y" * 1000}, artifact_id="a2")
    store = ArtifactStore(tmp_path / "cache", max_bytes=1500)
    store.materialize(verify_manifest(m1, k1, d1), d1, LocalFetcher(tmp_path / "s1"), "local")
    store.pin(d1)
    # Second artifact needs space; pinned d1 cannot be evicted, so this fails closed.
    with pytest.raises(Exception):
        store.materialize(verify_manifest(m2, k2, d2), d2, LocalFetcher(tmp_path / "s2"), "local")
    store.unpin(d1)
    store.materialize(verify_manifest(m2, k2, d2), d2, LocalFetcher(tmp_path / "s2"), "local")
    assert store.has(d2) and not store.has(d1)  # d1 evicted once unpinned
