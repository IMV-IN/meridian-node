"""Artifact management: signed manifest verification, digest checks, safe
staging, atomic promotion, and a pinned LRU cache (DESIGN.md 12).

Network transfer is abstracted behind `Fetcher` so the trust chain is testable
with local files; a real HTTPS/HF fetcher plugs in later. Verification never
trusts an HTTP 200 — only a matching digest and a valid signature.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Protocol

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import BaseModel, Field


class ArtifactError(Exception):
    pass


class VerificationError(ArtifactError):
    pass


# --- manifest model -----------------------------------------------------
class ManifestFile(BaseModel):
    model_config = {"extra": "forbid"}
    path: str
    size_bytes: int
    digest: str


class Signature(BaseModel):
    model_config = {"extra": "forbid"}
    algorithm: str
    key_id: str
    value: str


class Manifest(BaseModel):
    model_config = {"extra": "forbid"}
    manifest_version: int
    artifact_type: str
    artifact_id: str
    revision: str
    files: list[ManifestFile]
    total_size_bytes: int
    engine_image_digest: str
    runtime_requirements: dict = Field(default_factory=dict)
    source_allowlist: list[str] = Field(default_factory=list)
    signature: Signature


def _canonical_bytes(obj: dict) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def canonical_manifest_digest(manifest_dict: dict) -> str:
    body = {k: v for k, v in manifest_dict.items() if k != "signature"}
    return "sha256:" + hashlib.sha256(_canonical_bytes(body)).hexdigest()


def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def verify_manifest(manifest_dict: dict, trusted_keys: dict[str, bytes], expected_digest: str) -> Manifest:
    """Full manifest gate: digest identity, pinned signing key, valid signature.

    `trusted_keys` maps key_id -> raw Ed25519 public key bytes (the node's local
    policy pin). A centrally authenticated but unsigned or wrong-key manifest is
    rejected (DESIGN.md 12.1, 15.1).
    """
    actual_digest = canonical_manifest_digest(manifest_dict)
    if actual_digest != expected_digest:
        raise VerificationError(f"manifest digest mismatch: {actual_digest} != {expected_digest}")

    sig = manifest_dict.get("signature")
    if not sig or sig.get("algorithm") != "ed25519":
        raise VerificationError("manifest is unsigned or uses an unsupported algorithm")
    key_id = sig.get("key_id")
    if key_id not in trusted_keys:
        raise VerificationError(f"signing key {key_id!r} is not in local trust policy")

    body = {k: v for k, v in manifest_dict.items() if k != "signature"}
    try:
        Ed25519PublicKey.from_public_bytes(trusted_keys[key_id]).verify(
            _b64url_decode(sig["value"]), _canonical_bytes(body)
        )
    except InvalidSignature as e:
        raise VerificationError("manifest signature is invalid") from e

    return Manifest(**manifest_dict)


# --- transfer -----------------------------------------------------------
class Fetcher(Protocol):
    def fetch(self, source_uri: str, rel_path: str, dest: Path) -> None:
        """Write the file at source_uri/rel_path to dest. May resume/stream."""
        ...


def _safe_join(root: Path, rel: str) -> Path:
    if rel.startswith("/") or ".." in Path(rel).parts:
        raise VerificationError(f"unsafe manifest path: {rel!r}")
    target = (root / rel).resolve()
    if not str(target).startswith(str(root.resolve()) + os.sep):
        raise VerificationError(f"path escapes staging root: {rel!r}")
    return target


def _digest_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


# --- cache --------------------------------------------------------------
class ArtifactStore:
    def __init__(self, root: str | Path, max_bytes: int | None = None):
        self.root = Path(root)
        self.artifacts = self.root / "artifacts" / "sha256"
        self.staging = self.root / "staging"
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.staging.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        self._pinned: set[str] = set()

    def _digest_dir(self, manifest_digest: str) -> Path:
        return self.artifacts / manifest_digest.removeprefix("sha256:")

    def has(self, manifest_digest: str) -> bool:
        return self._digest_dir(manifest_digest).is_dir()

    def held(self) -> list[str]:
        """Digests currently materialized in the cache (for placement locality)."""
        return sorted("sha256:" + p.name for p in self.artifacts.iterdir() if p.is_dir())

    def path_for(self, manifest_digest: str) -> Path:
        d = self._digest_dir(manifest_digest)
        if not d.is_dir():
            raise ArtifactError(f"artifact not materialized: {manifest_digest}")
        return d

    def pin(self, manifest_digest: str) -> None:
        self._pinned.add(manifest_digest)

    def unpin(self, manifest_digest: str) -> None:
        self._pinned.discard(manifest_digest)

    def materialize(self, manifest: Manifest, manifest_digest: str, fetcher: Fetcher, source_uri: str) -> Path:
        """Fetch to staging, verify every file, then atomically promote.

        Idempotent: an already-materialized digest is returned as-is
        (DESIGN.md 11 idempotency, 12 rules).
        """
        if self.has(manifest_digest):
            return self.path_for(manifest_digest)

        self._ensure_space(manifest.total_size_bytes)
        stage = self.staging / manifest_digest.removeprefix("sha256:")
        if stage.exists():
            shutil.rmtree(stage)
        stage.mkdir(parents=True)
        try:
            total = 0
            for f in manifest.files:
                dest = _safe_join(stage, f.path)
                dest.parent.mkdir(parents=True, exist_ok=True)
                fetcher.fetch(source_uri, f.path, dest)
                if dest.is_symlink():
                    raise VerificationError(f"symlink not allowed: {f.path}")
                size = dest.stat().st_size
                if size != f.size_bytes:
                    raise VerificationError(f"size mismatch for {f.path}: {size} != {f.size_bytes}")
                actual = _digest_file(dest)
                if actual != f.digest:
                    raise VerificationError(f"digest mismatch for {f.path}: {actual} != {f.digest}")
                os.chmod(dest, 0o440)
                total += size
            if total != manifest.total_size_bytes:
                raise VerificationError(f"total size mismatch: {total} != {manifest.total_size_bytes}")

            final = self._digest_dir(manifest_digest)
            final.parent.mkdir(parents=True, exist_ok=True)
            os.rename(stage, final)  # atomic on same filesystem
            return final
        finally:
            if stage.exists():
                shutil.rmtree(stage, ignore_errors=True)

    def _dir_size(self, d: Path) -> int:
        return sum(p.stat().st_size for p in d.rglob("*") if p.is_file())

    def _ensure_space(self, needed: int) -> None:
        if self.max_bytes is None:
            return
        entries = [(d, self._dir_size(d), d.stat().st_mtime) for d in self.artifacts.iterdir() if d.is_dir()]
        used = sum(sz for _, sz, _ in entries)
        # Evict least-recently-used among unpinned artifacts only.
        for d, sz, _ in sorted(entries, key=lambda e: e[2]):
            if used + needed <= self.max_bytes:
                break
            digest = "sha256:" + d.name
            if digest in self._pinned:
                continue
            shutil.rmtree(d)
            used -= sz
        if used + needed > self.max_bytes:
            raise ArtifactError("insufficient artifact cache space after evicting unpinned entries")
