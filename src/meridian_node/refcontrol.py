"""Reference control-plane core (DESIGN.md 8.1, 10, 17.5).

This is the authoritative in-memory implementation of the server-side safety
rules: single-use tokens, approval gating, epoch fencing, the lease-expiry
takeover precondition, sequence monotonicity, and lease renewal on a
server-authoritative clock. It is shared by the dev/reference HTTP server
(devserver.py) and the test fake, so there is one implementation of the rules,
not two.

It is NOT the production control plane: no persistence, no real CA, no Postgres,
no gateway projection. The production `meridian-control` service lives in the
central Meridian repository (DESIGN.md 2, decision 13).
"""

from __future__ import annotations

import base64
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


class ControlError(Exception):
    """Server-side error carrying a wire error code (mirrors control.ControlError)."""

    def __init__(self, code: str, message: str, retryable: bool = False, http_status: int = 400):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable
        self.http_status = http_status


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


@dataclass
class Token:
    auto_approve: bool = False
    used: bool = False


@dataclass
class Claim:
    claim_id: str
    node_id: str
    public_key: bytes
    nonce: str  # hex string; the signed message is nonce.encode() (same on both transports)
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


class ControlCore:
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
        self.desired: dict[str, dict] = {}
        self.stop_authorized: dict[str, set] = {}
        self.observations: dict[str, dict] = {}

    # --- enrollment -----------------------------------------------------
    def enroll(self, token_str: Optional[str], request: dict) -> dict:
        token = self.tokens.get(token_str) if token_str else None
        if token is None:
            raise ControlError("INVALID_CREDENTIAL", "unknown enrollment token", http_status=401)
        if token.used:
            raise ControlError("INVALID_CREDENTIAL", "enrollment token already used", http_status=401)
        token.used = True

        node_id = "node_" + secrets.token_hex(12)
        public_key = _b64url_decode(request["node_public_key"])
        if token.auto_approve:
            self.nodes[node_id] = NodeRecord(node_id=node_id, public_key=public_key)
            return self._approved_response(node_id)

        claim_id = "claim_" + secrets.token_hex(12)
        self.claims[claim_id] = Claim(claim_id, node_id, public_key, secrets.token_hex(32))
        return {"status": "pending", "claim_id": claim_id}

    def claim_nonce(self, claim_id: str) -> str:
        claim = self.claims.get(claim_id)
        if claim is None:
            raise ControlError("NODE_NOT_FOUND", "unknown claim", http_status=404)
        return claim.nonce

    def resolve_claim(self, claim_id: str, signature: bytes) -> dict:
        claim = self.claims.get(claim_id)
        if claim is None:
            raise ControlError("NODE_NOT_FOUND", "unknown claim", http_status=404)
        try:
            Ed25519PublicKey.from_public_bytes(claim.public_key).verify(signature, claim.nonce.encode())
        except InvalidSignature as e:
            raise ControlError("INVALID_CREDENTIAL", "claim key possession proof failed", http_status=401) from e
        if claim.status == "denied":
            return {"status": "denied"}
        if claim.status == "approved":
            self.nodes.setdefault(claim.node_id, NodeRecord(claim.node_id, claim.public_key))
            return self._approved_response(claim.node_id)
        return {"status": "pending", "claim_id": claim_id}

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
            raise ControlError("NODE_NOT_FOUND", "unknown node", http_status=404)
        session_id = request["agent_session_id"]
        lease_valid = node.lease_expires_at is not None and self.now() < node.lease_expires_at
        has_active = node.active_session_id is not None and node.active_session_id != session_id
        if lease_valid and has_active and not request.get("handoff_token"):
            raise ControlError(
                "CONFLICT", "active session holds a valid lease; wait for expiry or supply a handoff token",
                http_status=409,
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
            raise ControlError("NODE_NOT_FOUND", "unknown node", http_status=404)
        if request["agent_session_id"] != node.active_session_id or request["fencing_epoch"] != node.fencing_epoch:
            raise ControlError("STALE_AGENT_SESSION", "agent session is no longer active", http_status=409)
        if request["sequence"] <= node.highest_sequence:
            raise ControlError("CONFLICT", "sequence is not greater than highest accepted", http_status=409)
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
        self.desired[node_id] = snapshot
        self.nodes[node_id].desired_generation = snapshot["generation"]

    def fetch_desired_state(self, node_id: str, generation: int) -> dict:
        if node_id not in self.nodes:
            raise ControlError("NODE_NOT_FOUND", "unknown node", http_status=404)
        snap = self.desired.get(node_id)
        if snap is None:
            raise ControlError("GENERATION_NOT_FOUND", "no desired state published", http_status=404)
        return snap

    def post_observation(self, node_id: str, observation: dict) -> dict:
        if node_id not in self.nodes:
            raise ControlError("NODE_NOT_FOUND", "unknown node", http_status=404)
        self.observations[node_id] = observation
        return {"received": True}
