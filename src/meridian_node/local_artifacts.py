"""Local artifact source for air-gapped mode (DESIGN.md 6.4, 12).

Reads signed manifests and artifact files from an inbox directory instead of over
HTTPS. Digest/signature verification is unchanged (artifacts.verify_manifest);
this layer only reads local bytes. Path traversal outside the artifact root is
rejected — a manifest file path may never escape the bundle.
"""

from __future__ import annotations

import json
from pathlib import Path

from .artifacts import VerificationError


class LocalFetcher:
    def __init__(self, artifacts_root: Path):
        self.artifacts_root = Path(artifacts_root).resolve()

    def fetch(self, source_uri: str, rel_path: str, dest: Path) -> None:
        # source_uri is the artifact root; rel_path comes from the (verified)
        # manifest. Reject any path that resolves outside the root.
        src = (Path(source_uri) / rel_path).resolve()
        if not src.is_relative_to(self.artifacts_root):
            raise VerificationError(f"artifact path escapes bundle root: {rel_path}")
        dest.write_bytes(src.read_bytes())


class LocalManifestSource:
    def __init__(self, inbox_dir: Path):
        self.inbox_dir = Path(inbox_dir)

    def get(self, manifest_digest: str, revision: str) -> tuple[dict, str]:
        digest = manifest_digest.removeprefix("sha256:")
        path = self.inbox_dir / "manifests" / f"{digest}.json"
        if not path.is_file():
            raise VerificationError(f"manifest not found in bundle: {path.name}")
        manifest = json.loads(path.read_text())
        # The artifact files live under the inbox artifacts root.
        return manifest, str((self.inbox_dir / "artifacts").resolve())
