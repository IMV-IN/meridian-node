"""HTTP artifact transfer (DESIGN.md 12). Requires the `http` extra.

Fetches signed manifests and model files over HTTPS, restricted to policy-approved
hosts, with redirects confined to the same allow-list. Verification of digests and
signatures happens in artifacts.py — this layer only moves bytes safely.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

try:
    import httpx
except ImportError as e:  # pragma: no cover
    raise ImportError("transfer requires the 'http' extra: pip install 'meridian-node[http]'") from e

from .artifacts import VerificationError


def _host_allowed(url: str, allowed_hosts: list[str]) -> bool:
    host = urlparse(url).hostname or ""
    return any(host == a or host.endswith("." + a) for a in allowed_hosts)


class HttpFetcher:
    def __init__(self, allowed_hosts: list[str], client: Optional["httpx.Client"] = None):
        self.allowed_hosts = allowed_hosts
        self._client = client or httpx.Client(follow_redirects=False, timeout=300.0)

    def fetch(self, source_uri: str, rel_path: str, dest: Path) -> None:
        url = urljoin(source_uri.rstrip("/") + "/", rel_path)
        if not _host_allowed(url, self.allowed_hosts):
            raise VerificationError(f"source host not in allow-list: {url}")
        with self._client.stream("GET", url) as resp:
            if resp.status_code in (301, 302, 303, 307, 308):
                target = resp.headers.get("location", "")
                if not _host_allowed(target, self.allowed_hosts):
                    raise VerificationError(f"redirect target not in allow-list: {target}")
                raise VerificationError("redirects must be pre-resolved to allow-listed hosts")
            if resp.status_code != 200:
                raise VerificationError(f"fetch failed: HTTP {resp.status_code} for {url}")
            with dest.open("wb") as f:
                for chunk in resp.iter_bytes(1024 * 1024):
                    f.write(chunk)


class HttpManifestSource:
    def __init__(self, base_url: str, allowed_hosts: list[str], client: Optional["httpx.Client"] = None):
        self.base_url = base_url.rstrip("/")
        self.allowed_hosts = allowed_hosts
        self._client = client or httpx.Client(follow_redirects=False, timeout=60.0)

    def get(self, manifest_digest: str, revision: str) -> tuple[dict, str]:
        digest = manifest_digest.removeprefix("sha256:")
        url = f"{self.base_url}/manifests/{digest}.json"
        if not _host_allowed(url, self.allowed_hosts):
            raise VerificationError(f"manifest host not in allow-list: {url}")
        resp = self._client.get(url)
        if resp.status_code != 200:
            raise VerificationError(f"manifest fetch failed: HTTP {resp.status_code}")
        manifest = resp.json()
        source_uri = manifest.get("source_allowlist", [self.base_url])[0]
        return manifest, source_uri
