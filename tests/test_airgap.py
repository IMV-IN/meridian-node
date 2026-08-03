"""Air-gapped mode (DESIGN.md 6.4): signed desired-state bundles, the offline
transport (inbox read / outbox spool / local epoch), and the full offline
managed loop that launches an engine from local artifacts with no network — the
Phase-4 gate ("validate local artifacts without registry access").
"""

from __future__ import annotations

import base64
import datetime as dt
import json

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from engine_helpers import FakeProber, make_assignment, make_signed_manifest

from meridian_node.agent import Agent
from meridian_node.airgap import BundleError, sign_desired_bundle, verify_desired_bundle
from meridian_node.artifacts import ArtifactStore
from meridian_node.config import Config
from meridian_node.control import ControlError
from meridian_node.journal import Journal
from meridian_node.local_artifacts import LocalFetcher, LocalManifestSource
from meridian_node.models import DesiredSnapshot
from meridian_node.models import EnginePhase as P
from meridian_node.offline_transport import OfflineTransport
from meridian_node.reconcile import Reconciler
from meridian_node.reservation import ReservationLedger
from meridian_node.runtime import FakeRuntime
from meridian_node.supervisor import EngineSupervisor

NOW = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def _authority():
    key = Ed25519PrivateKey.generate()
    pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
    return key, "auth1", {"auth1": base64.urlsafe_b64encode(pub).decode().rstrip("=")}


# --- bundle trust -------------------------------------------------------
def test_bundle_roundtrip():
    key, kid, keys = _authority()
    snap = {"node_id": "node_x", "generation": 3, "assignments": []}
    v = verify_desired_bundle(sign_desired_bundle(snap, 3, ["e1"], kid, key), keys)
    assert v.generation == 3 and v.snapshot == snap and v.stop_authorized_engine_ids == ["e1"]


def test_tampered_snapshot_rejected():
    key, kid, keys = _authority()
    bundle = sign_desired_bundle({"node_id": "node_x", "generation": 1}, 1, [], kid, key)
    bundle["snapshot"]["generation"] = 999  # tamper after signing
    with pytest.raises(BundleError, match="signature"):
        verify_desired_bundle(bundle, keys)


def test_unknown_authority_key_rejected():
    key, kid, keys = _authority()
    bundle = sign_desired_bundle({"node_id": "node_x", "generation": 1}, 1, [], kid, key)
    with pytest.raises(BundleError, match="unknown authority key"):
        verify_desired_bundle(bundle, {"someone-else": "AAAA"})


# --- offline transport --------------------------------------------------
def _write_bundle(inbox, name, key, kid, snap, gen, stop_ids=()):
    (inbox / "desired").mkdir(parents=True, exist_ok=True)
    (inbox / "desired" / name).write_text(json.dumps(sign_desired_bundle(snap, gen, list(stop_ids), kid, key)))


def test_offline_transport_reads_and_spools(tmp_path):
    key, kid, keys = _authority()
    inbox, outbox, state = tmp_path / "inbox", tmp_path / "outbox", tmp_path / "state"
    snap = {"node_id": "node_x", "generation": 5, "assignments": []}
    _write_bundle(inbox, "d.json", key, kid, snap, 5, ["e2"])
    t = OfflineTransport("node_x", inbox, outbox, keys, state, lease_ttl_seconds=30, now=lambda: NOW)

    hb = t.heartbeat("node_x", {"sequence": 1, "fencing_epoch": 1})
    assert hb["desired_generation"] == 5 and hb["stop_authorized_engine_ids"] == ["e2"]
    assert t.fetch_desired_state("node_x", 5) == snap
    assert t.establish_session("node_x", {})["fencing_epoch"] == 1
    assert t.establish_session("node_x", {})["fencing_epoch"] == 2  # monotonic across calls
    t.post_observation("node_x", {"observed_generation": 5, "sequence": 1, "engines": []})
    assert (outbox / "observation-5-1.json").is_file()


def test_offline_transport_ignores_foreign_and_invalid(tmp_path):
    key, kid, keys = _authority()
    inbox = tmp_path / "inbox"
    _write_bundle(inbox, "foreign.json", key, kid, {"node_id": "other", "generation": 9}, 9)
    (inbox / "desired" / "junk.json").write_text('{"not": "a bundle"}')
    t = OfflineTransport("node_x", inbox, tmp_path / "out", keys, tmp_path / "state", now=lambda: NOW)
    with pytest.raises(ControlError) as exc:
        t.fetch_desired_state("node_x", 0)  # nothing valid addressed to node_x
    assert exc.value.code == "GENERATION_NOT_FOUND"


def test_offline_transport_picks_highest_generation(tmp_path):
    key, kid, keys = _authority()
    inbox = tmp_path / "inbox"
    _write_bundle(inbox, "g1.json", key, kid, {"node_id": "node_x", "generation": 1}, 1)
    _write_bundle(inbox, "g3.json", key, kid, {"node_id": "node_x", "generation": 3}, 3)
    t = OfflineTransport("node_x", inbox, tmp_path / "out", keys, tmp_path / "state", now=lambda: NOW)
    assert t.fetch_desired_state("node_x", 0)["generation"] == 3


# --- full offline managed loop (Phase-4 gate) ---------------------------
def test_air_gapped_managed_loop(tmp_path):
    key, kid, keys = _authority()
    now = lambda: NOW  # noqa: E731
    node_id = "node_air"
    inbox, outbox, state = tmp_path / "inbox", tmp_path / "outbox", tmp_path / "state"
    artifacts_root = inbox / "artifacts"

    # Local artifact + signed manifest placed in the inbox (no registry).
    manifest, digest, trusted_keys, revision = make_signed_manifest(artifacts_root, {"model.bin": b"w"})
    (inbox / "manifests").mkdir(parents=True, exist_ok=True)
    (inbox / "manifests" / f"{digest.removeprefix('sha256:')}.json").write_text(json.dumps(manifest))

    a = make_assignment(engine_id="engine_1", generation=1, manifest_digest=digest, revision=revision)
    a.node_id = node_id
    snapshot = DesiredSnapshot(
        node_id=node_id, generation=1, snapshot_hash="sha256:" + "0" * 64, assignments=[a]
    ).model_dump(mode="json")
    _write_bundle(inbox, "desired.json", key, kid, snapshot, 1)

    cfg = Config(
        mode="air-gapped", state_dir=state,
        air_gap={"node_id": node_id, "inbox_dir": str(inbox), "outbox_dir": str(outbox), "authority_keys": keys},
        policy={"management_enabled": True},
    )
    transport = OfflineTransport(node_id, inbox, outbox, keys, state, now=now)
    agent = Agent(cfg, transport, now=now)
    assert agent.ensure_enrolled() == node_id  # identity from config, no network
    agent.establish_session()

    runtime = FakeRuntime()
    sup = EngineSupervisor(
        node_id=node_id, runtime=runtime, artifacts=ArtifactStore(state / "cache"),
        manifests=LocalManifestSource(inbox), prober=FakeProber(runtime), journal=Journal(state / "j.db"),
        reservations=ReservationLedger(device_capacity={"GPU-fake-0": 8 * 1024**3}, port_range=range(8000, 8100)),
        trusted_keys=trusted_keys, fetcher=LocalFetcher(artifacts_root), now=now,
    )
    agent.attach_reconciler(Reconciler(node_id, sup, agent.state, now=now))

    agent.tick()  # heartbeat -> verify bundle -> reconcile (materialize local artifact) -> spool
    assert sup.state("engine_1").phase == P.WARMING
    spooled = list(outbox.glob("observation-*.json"))
    assert spooled, "observation should be spooled to the outbox"

    runtime.mark_ready(sup.state("engine_1").handle.runtime_id, "test/model")
    agent.tick()
    assert sup.state("engine_1").phase == P.READY
