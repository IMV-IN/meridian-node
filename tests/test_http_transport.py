"""HTTP transport error-envelope mapping, exercised with httpx.MockTransport."""

from __future__ import annotations

import httpx
import pytest

from meridian_node.control import ControlError
from meridian_node.http_transport import HttpTransport


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_heartbeat_success_and_idempotency_key():
    seen = {}

    def handler(request):
        seen["key"] = request.headers.get("Idempotency-Key")
        return httpx.Response(200, json={"desired_generation": 3, "accepted_sequence": 5})

    t = HttpTransport("https://cp", client=_client(handler))
    resp = t.heartbeat("node_1", {"agent_session_id": "s1", "fencing_epoch": 2, "sequence": 5})
    assert resp["desired_generation"] == 3
    assert seen["key"] == "s1:2:5"


def test_stale_session_maps_to_typed_error():
    def handler(request):
        return httpx.Response(
            409, json={"error": {"code": "STALE_AGENT_SESSION", "message": "gone", "retryable": False}}
        )

    t = HttpTransport("https://cp", client=_client(handler))
    with pytest.raises(ControlError) as exc:
        t.heartbeat("node_1", {"agent_session_id": "s1", "fencing_epoch": 2, "sequence": 5})
    assert exc.value.code == "STALE_AGENT_SESSION"
    assert exc.value.retryable is False


def test_rate_limited_is_retryable():
    def handler(request):
        return httpx.Response(429, json={"error": {"code": "RATE_LIMITED", "message": "slow down"}})

    t = HttpTransport("https://cp", client=_client(handler))
    with pytest.raises(ControlError) as exc:
        t.heartbeat("node_1", {"agent_session_id": "s1", "fencing_epoch": 2, "sequence": 5})
    assert exc.value.retryable is True


def test_enroll_sends_bearer_token():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, json={"status": "pending", "claim_id": "claim_1"})

    t = HttpTransport("https://cp", enrollment_token="secret-token", client=_client(handler))
    resp = t.enroll({"node_public_key": "k"})
    assert resp["claim_id"] == "claim_1"
    assert seen["auth"] == "Bearer secret-token"
