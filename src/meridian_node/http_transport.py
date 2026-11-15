"""Real HTTPS transport (DESIGN.md 10). Requires the `http` extra (httpx).

Carries the node mTLS client certificate on every call after enrollment and
maps the wire error envelope to typed `ControlError`s. The enrollment token is
sent only on the enroll call (DESIGN.md 10.1). The possession-proof handshake
for a pending claim is finalized against the central Meridian server; the shape
here matches contracts/v1 and the fake control plane.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import urllib.parse
from typing import IO, Callable, Optional

try:
    import httpx
except ImportError as e:  # pragma: no cover - import guard
    raise ImportError("http_transport requires the 'http' extra: pip install 'meridian-node[http]'") from e

from .control import ControlError

_RETRYABLE_CODES = {"RATE_LIMITED", "CONTROL_UNAVAILABLE"}
# Task downloads (docs/COMPUTE_TASKS_NODE.md 3.4): 30 s between reads, 300 s in all.
TASK_READ_TIMEOUT = 30.0
TASK_TOTAL_TIMEOUT = 300.0
MAX_ERROR_BODY = 64 * 1024
# Path segments are built only from values of these shapes (models.JOB_ID; node ids are opaque ids too).
_PATH_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def _task_error(status: int, body: bytes) -> ControlError:
    """The contracts/v1 error envelope of a failed task call. 5xx is always retryable."""
    try:
        err = json.loads(body).get("error", {})
        code, message = str(err.get("code", "")), str(err.get("message", ""))[:256]
    except Exception:
        code, message = "", ""
    code = code or ("CONTROL_UNAVAILABLE" if status >= 500 else f"HTTP_{status}")
    return ControlError(code, message or f"HTTP {status}", retryable=status >= 500 or code in _RETRYABLE_CODES)


def _raise_for_envelope(resp: "httpx.Response") -> dict:
    if resp.status_code < 400:
        return dict(resp.json())
    try:
        err = resp.json().get("error", {})
    except Exception:
        raise ControlError("CONTROL_UNAVAILABLE", f"HTTP {resp.status_code}", retryable=True)
    code = err.get("code", "CONTROL_UNAVAILABLE")
    raise ControlError(code, err.get("message", ""), retryable=err.get("retryable", code in _RETRYABLE_CODES))


class HttpTransport:
    def __init__(
        self,
        base_url: str,
        enrollment_token: Optional[str] = None,
        client: Optional["httpx.Client"] = None,
        cert: Optional[tuple[str, str]] = None,
        verify: str | bool = True,
        timeout: float = 35.0,
        client_cert_pem: Optional[str] = None,
        client_cert_header: str = "X-Client-Cert",
    ):
        self.base_url = base_url.rstrip("/")
        self.enrollment_token = enrollment_token
        # `cert` is (client_cert_path, client_key_path) for node mTLS; `verify`
        # points at the trust bundle. Injected `client` is used by tests.
        self._client = client or httpx.Client(cert=cert, verify=verify, timeout=timeout)
        # The url-escaped node cert PEM, forwarded (XFCC-style) so the control
        # plane can bind identity when the edge terminates TLS (DESIGN.md 15.6).
        self._auth = (
            {client_cert_header: urllib.parse.quote(client_cert_pem)} if client_cert_pem else {}
        )

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def enroll(self, request: dict) -> dict:
        headers = {"Authorization": f"Bearer {self.enrollment_token}"} if self.enrollment_token else {}
        resp = self._client.post(self._url("/control/v1/enroll"), json=request, headers=headers)
        return _raise_for_envelope(resp)

    def get_claim(self, claim_id: str, signed_nonce_provider: Callable[[bytes], str]) -> dict:
        # Step 1: obtain the server nonce. Step 2: return it signed.
        probe = _raise_for_envelope(self._client.get(self._url(f"/control/v1/enroll/claims/{claim_id}")))
        if probe.get("status") != "pending" or "nonce" not in probe:
            return probe
        nonce = probe["nonce"].encode()
        resp = self._client.get(
            self._url(f"/control/v1/enroll/claims/{claim_id}"),
            headers={"X-Possession-Proof": signed_nonce_provider(nonce)},
        )
        return _raise_for_envelope(resp)

    def establish_session(self, node_id: str, request: dict) -> dict:
        resp = self._client.post(
            self._url(f"/control/v1/nodes/{node_id}/sessions"), json=request, headers=self._auth
        )
        return _raise_for_envelope(resp)

    def heartbeat(self, node_id: str, request: dict) -> dict:
        key = f"{request['agent_session_id']}:{request['fencing_epoch']}:{request['sequence']}"
        resp = self._client.post(
            self._url(f"/control/v1/nodes/{node_id}/heartbeat"),
            json=request,
            headers={"Idempotency-Key": key, **self._auth},
        )
        return _raise_for_envelope(resp)

    def fetch_desired_state(self, node_id: str, generation: int) -> dict:
        resp = self._client.get(
            self._url(f"/control/v1/nodes/{node_id}/desired-state"),
            params={"generation": generation},
            headers=self._auth,
        )
        return _raise_for_envelope(resp)

    def post_observation(self, node_id: str, observation: dict) -> dict:
        resp = self._client.post(
            self._url(f"/control/v1/nodes/{node_id}/observations"), json=observation, headers=self._auth
        )
        return _raise_for_envelope(resp)

    def rotate_certificate(self, node_id: str) -> dict:
        resp = self._client.post(
            self._url(f"/control/v1/nodes/{node_id}/certificate"), headers=self._auth
        )
        return _raise_for_envelope(resp)

    def _task_url(self, node_id: str, job_id: str, replica: int, tail: str) -> str:
        if not (isinstance(node_id, str) and _PATH_ID.fullmatch(node_id) and isinstance(job_id, str)
                and _PATH_ID.fullmatch(job_id) and type(replica) is int and replica >= 0):
            raise ValueError("invalid task path segment")
        return self._url(f"/control/v1/nodes/{node_id}/tasks/{job_id}/replicas/{replica}/{tail}")

    def _task_headers(self, session_id: str, epoch: int) -> dict[str, str]:
        if not isinstance(session_id, str) or not _PATH_ID.fullmatch(session_id) or type(epoch) is not int:
            raise ValueError("invalid session")
        return {**self._auth, "X-Meridian-Agent-Session": session_id, "X-Meridian-Fencing-Epoch": str(epoch)}

    def fetch_task_blob(
        self, node_id: str, job_id: str, replica: int, kind: str, dest: IO[bytes], max_bytes: int,
        session_id: str, epoch: int,
    ) -> tuple[str, int]:
        if kind not in ("bundle", "payload"):
            raise ValueError("kind must be bundle or payload")
        url = self._task_url(node_id, job_id, replica, kind)
        headers = self._task_headers(session_id, epoch)
        deadline = time.monotonic() + TASK_TOTAL_TIMEOUT
        sha, size = hashlib.sha256(), 0
        try:
            with self._client.stream("GET", url, headers=headers, timeout=TASK_READ_TIMEOUT) as resp:
                if resp.status_code != 200:
                    body = b""
                    for chunk in resp.iter_bytes():
                        body += chunk
                        if len(body) > MAX_ERROR_BODY or time.monotonic() > deadline:
                            break
                    raise _task_error(resp.status_code, body[:MAX_ERROR_BODY])
                length = resp.headers.get("Content-Length")
                if length is not None and (not length.isdigit() or int(length) > max_bytes):
                    raise ControlError("DIGEST_MISMATCH", "Content-Length disagrees with the snapshot")
                for chunk in resp.iter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise ControlError("DIGEST_MISMATCH", "more bytes than the snapshot's size_bytes")
                    if time.monotonic() > deadline:
                        raise ControlError("CONTROL_UNAVAILABLE", "download exceeded 300 s", retryable=True)
                    sha.update(chunk)
                    dest.write(chunk)
                digest = "sha256:" + sha.hexdigest()
                claimed = resp.headers.get("X-Meridian-Content-Digest")
                if (claimed is not None and claimed != digest) or (length is not None and int(length) != size):
                    raise ControlError("DIGEST_MISMATCH", "response headers disagree with the bytes")
                return digest, size
        except httpx.HTTPError as e:
            raise ControlError("CONTROL_UNAVAILABLE", f"download failed: {type(e).__name__}", retryable=True) from None

    def upload_task_result(
        self, node_id: str, job_id: str, replica: int, kind: str, data: bytes, digest: str,
        session_id: str, epoch: int,
    ) -> dict:
        if kind not in ("result", "error") or not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise ValueError("invalid result kind or digest")
        url = self._task_url(node_id, job_id, replica, "result")
        headers = {
            **self._task_headers(session_id, epoch),
            "Content-Type": "application/octet-stream", "X-Meridian-Content-Digest": digest,
        }
        try:
            resp = self._client.put(url, params={"kind": kind}, content=data, headers=headers,
                                    timeout=TASK_READ_TIMEOUT)
        except httpx.HTTPError as e:
            raise ControlError("CONTROL_UNAVAILABLE", f"upload failed: {type(e).__name__}", retryable=True) from None
        if resp.status_code in (200, 201):
            return {"stored": resp.status_code == 201}
        raise _task_error(resp.status_code, resp.content[:MAX_ERROR_BODY])

    def close(self) -> None:
        self._client.close()
