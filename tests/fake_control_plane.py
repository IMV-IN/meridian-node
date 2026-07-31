"""In-process fake control plane for Phase 0 tests.

It enforces the safety-critical server-side rules from DESIGN.md sections 8.1,
10, and 17.5: single-use enrollment tokens, approval gating, fenced sessions
with a monotonic epoch, the lease-expiry precondition on session
establishment, monotonic heartbeat sequences, and lease renewal on a
server-authoritative clock. It is the reference the agent is tested against; a
real mTLS HTTP server must reproduce the same behavior.
"""

from __future__ import annotations

import base64
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from meridian_node.control import ControlError


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


@dataclass
class Token:
    auto_approve: bool = False
    used: bool = False


@dataclass
class Claim:
    claim_id: str
    node_id: str
    public_key: bytes
    nonce: bytes
    status: str = "pending"  # pending | approved | denied


@dataclass
class NodeRecord:
    node_id: str
    public_key: bytes
    active_session_id: Optional[str] = None
    fencing_epoch: int = 0
    highest_sequence: int = 0
    lease_expires_at: Optional[datetime] = None
    desired_generation: int = 0
    used_session_ids: set[str] = field(default_factory=set)


class FakeControlPlane:
    def __init__(
        self,
        tokens: dict[str, Token],
        now: Callable[[], datetime] = _utcnow,
        lease_ttl_seconds: int = 30,
    ):
        self.tokens = tokens
        self.now = now
        self.lease_ttl = timedelta(seconds=lease_ttl_seconds)
        self.nodes: dict[str, NodeRecord] = {}
        self.claims: dict[str, Claim] = {}
        self._enroll_token: Optional[str] = None  # set by tests via with_token()
        self.desired: dict[str, dict] = {}  # node_id -> snapshot dict
        self.stop_authorized: dict[str, set] = {}  # node_id -> engine_ids cleared to stop
        self.observations: dict[str, dict] = {}  # node_id -> latest observation

    # Tests select which token the next enroll() call presents.
    def presenting(self, token: str) -> "FakeControlPlane":
        self._enroll_token = token
        return self

    # --- enrollment -----------------------------------------------------
    def enroll(self, request: dict) -> dict:
        token_str = self._enroll_token
        token = self.tokens.get(token_str) if token_str else None
        if token is None:
            raise ControlError("INVALID_CREDENTIAL", "unknown enrollment token")
        if token.used:
            raise ControlError("INVALID_CREDENTIAL", "enrollment token already used")
        token.used = True

        node_id = "node_" + secrets.token_hex(12)
        public_key = _b64url_decode(request["node_public_key"])

        if token.auto_approve:
            self.nodes[node_id] = NodeRecord(node_id=node_id, public_key=public_key)
            return self._approved_response(node_id)

        claim_id = "claim_" + secrets.token_hex(12)
        self.claims[claim_id] = Claim(
            claim_id=claim_id, node_id=node_id, public_key=public_key, nonce=secrets.token_bytes(32)
        )
        return {"status": "pending", "claim_id": claim_id}

    def get_claim(self, claim_id: str, signed_nonce_provider: Callable[[bytes], str]) -> dict:
        claim = self.claims.get(claim_id)
        if claim is None:
            raise ControlError("NODE_NOT_FOUND", "unknown claim")
        # Proof of key possession before we ever hand back a certificate.
        signature = _b64url_decode(signed_nonce_provider(claim.nonce))
        try:
            Ed25519PublicKey.from_public_bytes(claim.public_key).verify(signature, claim.nonce)
        except InvalidSignature as e:
            raise ControlError("INVALID_CREDENTIAL", "claim key possession proof failed") from e
        if claim.status == "denied":
            return {"status": "denied"}
        if claim.status == "approved":
            self.nodes.setdefault(claim.node_id, NodeRecord(node_id=claim.node_id, public_key=claim.public_key))
            return self._approved_response(claim.node_id)
        return {"status": "pending", "claim_id": claim_id}

    # Operator actions (out-of-band in production).
    def approve_claim(self, claim_id: str) -> None:
        self.claims[claim_id].status = "approved"

    def deny_claim(self, claim_id: str) -> None:
        self.claims[claim_id].status = "denied"

    def _approved_response(self, node_id: str) -> dict:
        return {
            "status": "approved",
            "node_id": node_id,
            "certificate": f"FAKE-CERT-{node_id}",
            "trust_bundle": "FAKE-TRUST-BUNDLE",
            "protocol_version": "v1",
            "lease": {"heartbeat_interval_seconds": 10, "lease_ttl_seconds": int(self.lease_ttl.total_seconds())},
        }

    # --- session --------------------------------------------------------
    def establish_session(self, node_id: str, request: dict) -> dict:
        node = self.nodes.get(node_id)
        if node is None:
            raise ControlError("NODE_NOT_FOUND", "unknown node")
        session_id = request["agent_session_id"]

        lease_valid = node.lease_expires_at is not None and self.now() < node.lease_expires_at
        has_active = node.active_session_id is not None and node.active_session_id != session_id
        if lease_valid and has_active and not request.get("handoff_token"):
            raise ControlError(
                "CONFLICT", "active session holds a valid lease; wait for expiry or supply a handoff token"
            )

        node.fencing_epoch += 1
        node.active_session_id = session_id
        node.used_session_ids.add(session_id)
        node.lease_expires_at = self.now() + self.lease_ttl
        return {"fencing_epoch": node.fencing_epoch, "lease_expires_at": _iso(node.lease_expires_at)}

    # --- heartbeat ------------------------------------------------------
    def heartbeat(self, node_id: str, request: dict) -> dict:
        node = self.nodes.get(node_id)
        if node is None:
            raise ControlError("NODE_NOT_FOUND", "unknown node")
        if request["agent_session_id"] != node.active_session_id or request["fencing_epoch"] != node.fencing_epoch:
            raise ControlError("STALE_AGENT_SESSION", "agent session is no longer active")
        if request["sequence"] <= node.highest_sequence:
            raise ControlError("CONFLICT", "sequence is not greater than highest accepted")

        node.highest_sequence = request["sequence"]
        node.lease_expires_at = self.now() + self.lease_ttl
        return {
            "server_time": _iso(self.now()),
            "accepted_sequence": request["sequence"],
            "fencing_epoch": node.fencing_epoch,
            "lease_expires_at": _iso(node.lease_expires_at),
            "desired_generation": node.desired_generation,
            "admission_projection_revision": 0,
            "stop_authorized_engine_ids": sorted(self.stop_authorized.get(node_id, set())),
        }

    # --- desired state / observations ----------------------------------
    def set_desired(self, node_id: str, snapshot: dict) -> None:
        """Operator/placement action: publish a desired snapshot for a node."""
        self.desired[node_id] = snapshot
        self.nodes[node_id].desired_generation = snapshot["generation"]

    def fetch_desired_state(self, node_id: str, generation: int) -> dict:
        if node_id not in self.nodes:
            raise ControlError("NODE_NOT_FOUND", "unknown node")
        snap = self.desired.get(node_id)
        if snap is None:
            raise ControlError("GENERATION_NOT_FOUND", "no desired state published")
        return snap

    def post_observation(self, node_id: str, observation: dict) -> dict:
        if node_id not in self.nodes:
            raise ControlError("NODE_NOT_FOUND", "unknown node")
        self.observations[node_id] = observation
        return {"received": True}
