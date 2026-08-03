"""Real HTTPS transport (DESIGN.md 10). Requires the `http` extra (httpx).

Carries the node mTLS client certificate on every call after enrollment and
maps the wire error envelope to typed `ControlError`s. The enrollment token is
sent only on the enroll call (DESIGN.md 10.1). The possession-proof handshake
for a pending claim is finalized against the central Meridian server; the shape
here matches contracts/v1 and the fake control plane.
"""

from __future__ import annotations

import urllib.parse
from typing import Callable, Optional

try:
    import httpx
except ImportError as e:  # pragma: no cover - import guard
    raise ImportError("http_transport requires the 'http' extra: pip install 'meridian-node[http]'") from e

from .control import ControlError

_RETRYABLE_CODES = {"RATE_LIMITED", "CONTROL_UNAVAILABLE"}


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

    def close(self) -> None:
        self._client.close()
