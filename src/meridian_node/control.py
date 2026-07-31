"""Control-plane client and transport contract.

The transport is abstract so Phase 0 can drive the agent against an in-process
fake control plane (tests/fake_control_plane.py) without a running server. A
real mTLS HTTP transport implements the same three calls in a later step; the
fencing and sequence rules live on the server side, which the fake enforces
faithfully.
"""

from __future__ import annotations

from typing import Protocol


class ControlError(Exception):
    """A typed control-plane error mapped from the wire error envelope."""

    def __init__(self, code: str, message: str, retryable: bool = False):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.retryable = retryable


class Transport(Protocol):
    def enroll(self, request: dict) -> dict:
        """POST /control/v1/enroll. May return status 'pending' or 'approved'."""
        ...

    def get_claim(self, claim_id: str, signed_nonce_provider) -> dict:
        """GET /control/v1/enroll/claims/{claim_id}. Polls a pending claim."""
        ...

    def establish_session(self, node_id: str, request: dict) -> dict:
        """POST /control/v1/nodes/{node_id}/sessions. Returns fencing_epoch."""
        ...

    def heartbeat(self, node_id: str, request: dict) -> dict:
        """POST /control/v1/nodes/{node_id}/heartbeat."""
        ...
