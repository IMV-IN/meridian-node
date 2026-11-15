"""Control-plane client and transport contract.

The transport is abstract so Phase 0 can drive the agent against an in-process
fake control plane (tests/fake_control_plane.py) without a running server. A
real mTLS HTTP transport implements the same three calls in a later step; the
fencing and sequence rules live on the server side, which the fake enforces
faithfully.
"""

from __future__ import annotations

from typing import IO, Protocol


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

    def fetch_desired_state(self, node_id: str, generation: int) -> dict:
        """GET /control/v1/nodes/{node_id}/desired-state. Complete snapshot."""
        ...

    def post_observation(self, node_id: str, observation: dict) -> dict:
        """POST /control/v1/nodes/{node_id}/observations."""
        ...

    def rotate_certificate(self, node_id: str) -> dict:
        """POST /control/v1/nodes/{node_id}/certificate. Returns a fresh cert for
        the node's registered public key (Phase 4 cert rotation)."""
        ...

    def fetch_task_blob(
        self, node_id: str, job_id: str, replica: int, kind: str, dest: IO[bytes], max_bytes: int,
        session_id: str, epoch: int,
    ) -> tuple[str, int]:
        """GET .../tasks/{job_id}/replicas/{replica}/{bundle|payload}, streamed into `dest`.
        Aborts past `max_bytes`. Returns ("sha256:<hex>", size) of the bytes written
        (docs/COMPUTE_TASKS_NODE.md 3.4). A size or header that disagrees is DIGEST_MISMATCH."""
        ...

    def upload_task_result(
        self, node_id: str, job_id: str, replica: int, kind: str, data: bytes, digest: str,
        session_id: str, epoch: int,
    ) -> dict:
        """PUT .../tasks/{job_id}/replicas/{replica}/result?kind={result|error}."""
        ...
