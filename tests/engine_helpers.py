"""Shared builders for engine-layer tests: real signed manifests, on-disk
artifact files, a local fetcher, and readiness-aware fakes."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from meridian_node.artifacts import canonical_manifest_digest
from meridian_node.drivers import ReadinessSpec
from meridian_node.models import Assignment
from meridian_node.runtime import FakeRuntime

KEY_ID = "test-key-1"
IMAGE_DIGEST = "sha256:" + "a" * 64


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def make_signed_manifest(source_dir: Path, files: dict[str, bytes], artifact_id="artifact_x", revision="rev1"):
    """Write files to source_dir, build+sign a manifest. Returns everything a
    test needs: (manifest_dict, manifest_digest, trusted_keys, revision)."""
    source_dir.mkdir(parents=True, exist_ok=True)
    file_entries = []
    total = 0
    for name, content in files.items():
        (source_dir / name).write_bytes(content)
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        file_entries.append({"path": name, "size_bytes": len(content), "digest": digest})
        total += len(content)

    body = {
        "manifest_version": 1,
        "artifact_type": "model-runtime",
        "artifact_id": artifact_id,
        "revision": revision,
        "files": file_entries,
        "total_size_bytes": total,
        "engine_image_digest": IMAGE_DIGEST,
        "runtime_requirements": {},
        "source_allowlist": ["local"],
    }
    manifest_digest = canonical_manifest_digest(body)
    key = Ed25519PrivateKey.generate()
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    signature = key.sign(canonical)
    manifest = {**body, "signature": {"algorithm": "ed25519", "key_id": KEY_ID, "value": _b64url(signature)}}
    pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
    return manifest, manifest_digest, {KEY_ID: pub}, revision


class LocalFetcher:
    def __init__(self, source_dir: Path):
        self.source_dir = source_dir

    def fetch(self, source_uri: str, rel_path: str, dest: Path) -> None:
        dest.write_bytes((self.source_dir / rel_path).read_bytes())


class FakeManifestSource:
    def __init__(self, manifest: dict):
        self._manifest = manifest

    def get(self, manifest_digest: str, revision: str):
        return self._manifest, "local"


class FakeProber:
    """Ready when the fake container is marked ready and serves the model."""

    def __init__(self, runtime: FakeRuntime):
        self.runtime = runtime

    def probe(self, host: str, port: int, spec: ReadinessSpec) -> bool:
        for c in self.runtime._containers.values():  # test-only introspection
            if c.status.port == port and c.status.alive:
                return c.ready and c.served_model == spec.served_model
        return False


def build_supervisor(tmp_path, runtime, manifest, trusted_keys, source_dir, now, node_id="node_test"):
    from meridian_node.artifacts import ArtifactStore
    from meridian_node.journal import Journal
    from meridian_node.reservation import ReservationLedger
    from meridian_node.supervisor import EngineSupervisor

    store = ArtifactStore(tmp_path / "cache")
    journal = Journal(tmp_path / "journal.db")
    ledger = ReservationLedger(device_capacity={"GPU-fake-0": 8 * 1024**3}, port_range=range(8000, 8100))
    sup = EngineSupervisor(
        node_id=node_id,
        runtime=runtime,
        artifacts=store,
        manifests=FakeManifestSource(manifest),
        prober=FakeProber(runtime),
        journal=journal,
        reservations=ledger,
        trusted_keys=trusted_keys,
        fetcher=LocalFetcher(source_dir),
        now=now,
    )
    return sup


def make_assignment(
    engine_id="engine_1", generation=1, manifest_digest="", revision="rev1", port=8000, device="GPU-fake-0"
) -> Assignment:
    return Assignment(
        engine_id=engine_id,
        node_id="node_test",
        generation=generation,
        spec_revision=1,
        desired_phase="Running",
        engine={"kind": "vllm", "image_digest": IMAGE_DIGEST, "version": "0.10.2"},
        artifact={
            "artifact_id": "artifact_x",
            "revision": revision,
            "manifest_digest": manifest_digest,
            "engine_image_digest": IMAGE_DIGEST,
        },
        resources={"device_ids": [device], "memory_limit_bytes": 2 * 1024**3, "port": port},
        parameters={"served_model_name": "test/model", "max_model_len": 4096},
        policy={"restart": {"max_attempts": 2, "backoff_seconds": 10}, "drain_timeout_seconds": 30},
    )
