"""Agent orchestration for Phase 0: enroll, establish session, heartbeat.

No engine process control exists here (DESIGN.md 21, Phase 0). The agent owns
durable identity and state, establishes a fenced session, and renews its lease.
A restart constructs a fresh Agent against the same state directory; identity
and accepted generation are recovered, a new session ID is minted, and the
heartbeat sequence continues monotonically.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from cryptography import x509

from . import AGENT_VERSION, PROTOCOL_VERSION
from .config import Config
from .control import ControlError, Transport
from .identity import NodeIdentity, new_session_id
from .inventory import FakeNvidiaProvider, InventoryProvider, inventory_digest
from .state import StateStore


def _read_host_boot_id() -> str:
    p = Path("/proc/sys/kernel/random/boot_id")
    try:
        return p.read_text().strip()
    except OSError:
        return "boot-unknown"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Agent:
    def __init__(
        self,
        config: Config,
        transport: Transport,
        inventory: Optional[InventoryProvider] = None,
        now: Callable[[], datetime] = _utcnow,
    ):
        self.config = config
        self.transport = transport
        self.inventory = inventory or FakeNvidiaProvider()
        self.now = now
        state_dir = Path(config.state_dir)
        self.identity = NodeIdentity.load_or_create(state_dir / "node.key")
        self.state = StateStore(state_dir / "state.db")
        self.host_boot_id = _read_host_boot_id()
        self.session_id = new_session_id()
        self.fencing_epoch: Optional[int] = None
        self.lease_expires_at: Optional[str] = None

        row = self.state.load_identity()
        self.node_id: Optional[str] = row[0] if row else None
        self.certificate: Optional[str] = row[1] if row else None

        # Reconciler is attached only in managed-host mode (Phase 2+). Observe-only
        # agents never construct one, so they cannot start or stop engines.
        self.reconciler = None
        self._cached_snapshot: Optional[dict] = None

    # --- enrollment -----------------------------------------------------
    def ensure_enrolled(self, poll_interval: float = 0.0, max_polls: int = 100) -> str:
        """Enroll if no durable identity exists. Handles pending approval, and
        resumes a persisted pending claim rather than re-spending a used token."""
        if self.node_id:
            return self.node_id

        claim_id = self.state.pending_claim()
        if claim_id is None:
            request = {
                "agent_version": AGENT_VERSION,
                "protocol_versions": [PROTOCOL_VERSION],
                "node_public_key": self.identity.public_key_b64url(),
                "host_claims": {"hostname": self.config.display_name or "", "platform": "linux"},
                "host_boot_id": self.host_boot_id,
                "agent_session_id": self.session_id,
            }
            resp = self.transport.enroll(request)
            if resp["status"] == "approved":
                return self._accept_enrollment(resp)
            claim_id = resp["claim_id"]
            self.state.set_pending_claim(claim_id)

        resp = self._await_claim(claim_id, poll_interval, max_polls)
        return self._accept_enrollment(resp)

    def _accept_enrollment(self, resp: dict) -> str:
        self.node_id = resp["node_id"]
        self._persist_certificate(resp["certificate"], resp.get("trust_bundle"))
        self.state.clear_pending_claim()
        return self.node_id

    def _persist_certificate(self, certificate: str, trust_bundle: Optional[str]) -> None:
        """Persist the mTLS material so a restart can present the client cert
        without re-enrolling (DESIGN.md 15.6). Key already lives at node.key."""
        assert self.node_id is not None
        self.certificate = certificate
        self.state.save_identity(self.node_id, certificate, _iso(self.now()))
        state_dir = Path(self.config.state_dir)
        if certificate:  # air-gapped mode has no cert; don't write an empty file
            (state_dir / "node.crt").write_text(certificate)
        if trust_bundle:
            (state_dir / "trust_bundle.pem").write_text(trust_bundle)

    def maybe_rotate_certificate(self, renew_before_fraction: float = 0.5) -> bool:
        """Renew the node cert once it is past `renew_before_fraction` of its
        lifetime, so an enforced-mTLS session never breaks on expiry (Phase 4).
        Returns True if a new cert was obtained (the caller must then rebuild the
        transport to present it). Unparseable certs (dev/reference fakes) skip."""
        if not self.node_id or not self.certificate:
            return False
        try:
            cert = x509.load_pem_x509_certificate(self.certificate.encode())
            lifetime = cert.not_valid_after_utc - cert.not_valid_before_utc
            remaining = cert.not_valid_after_utc - self.now()
        except (ValueError, TypeError):
            return False
        if remaining > lifetime * renew_before_fraction:
            return False
        resp = self.transport.rotate_certificate(self.node_id)
        self._persist_certificate(resp["certificate"], resp.get("trust_bundle"))
        return True

    def _await_claim(self, claim_id: str, poll_interval: float, max_polls: int) -> dict:
        for _ in range(max_polls):
            resp = self.transport.get_claim(claim_id, self.identity.sign_b64url)
            if resp["status"] == "approved":
                return resp
            if resp["status"] == "denied":
                self.state.clear_pending_claim()
                raise ControlError("CLAIM_DENIED", "enrollment claim was denied")
            if poll_interval:
                time.sleep(poll_interval)
        raise ControlError("PENDING_APPROVAL", "enrollment claim not approved in time", retryable=True)

    # --- session --------------------------------------------------------
    def establish_session(self, handoff_token: Optional[str] = None) -> int:
        """Establish a fenced session. Rejected while a prior lease is valid."""
        assert self.node_id, "must enroll before establishing a session"
        request = {
            "agent_session_id": self.session_id,
            "host_boot_id": self.host_boot_id,
            "handoff_token": handoff_token,
        }
        resp = self.transport.establish_session(self.node_id, request)
        self.fencing_epoch = resp["fencing_epoch"]
        return self.fencing_epoch

    # --- heartbeat ------------------------------------------------------
    def heartbeat_once(self) -> dict:
        assert self.node_id and self.fencing_epoch is not None, "session not established"
        gpus = self.inventory.gpus()
        sequence = self.state.last_sequence() + 1
        request = {
            "host_boot_id": self.host_boot_id,
            "agent_session_id": self.session_id,
            "fencing_epoch": self.fencing_epoch,
            "sequence": sequence,
            "sent_at": _iso(self.now()),
            "agent": {"version": AGENT_VERSION, "protocol": PROTOCOL_VERSION, "mode": self.config.mode},
            "inventory_digest": inventory_digest(gpus),
            "gpus": gpus,
            "artifacts": [],
            "engines": [],
            "conditions": [],
        }
        resp = self.transport.heartbeat(self.node_id, request)
        # Persist only after the server accepts and durably commits the sequence.
        self.state.set_last_sequence(sequence)
        if self.reconciler is None:
            # Observe-only: the heartbeat is the only place generation is tracked.
            self.state.set_accepted_generation(resp["desired_generation"])
        self.lease_expires_at = resp["lease_expires_at"]
        return resp

    def attach_reconciler(self, reconciler) -> None:
        """Enable reconciliation. Refused unless the mode launches engines."""
        if self.config.mode not in ("managed-host", "air-gapped"):
            raise ValueError(f"reconciler requires an engine-launching mode, not {self.config.mode!r}")
        self.reconciler = reconciler

    def tick(self) -> dict:
        """One managed-host cycle: heartbeat, fetch desired state if the
        generation advanced, reconcile, and report observations (DESIGN.md 11)."""
        resp = self.heartbeat_once()
        if self.reconciler is None:
            return resp

        from .models import DesiredSnapshot

        desired_generation = resp["desired_generation"]
        if desired_generation > 0 and (
            self._cached_snapshot is None or desired_generation > self._cached_snapshot["generation"]
        ):
            self._cached_snapshot = self.transport.fetch_desired_state(self.node_id, desired_generation)

        if self._cached_snapshot is not None:
            snapshot = DesiredSnapshot(**self._cached_snapshot)
            stop_authorized = set(resp.get("stop_authorized_engine_ids", []))
            observation = self.reconciler.apply(snapshot, stop_authorized=stop_authorized)
            observation["agent_session_id"] = self.session_id
            observation["sequence"] = self.state.last_sequence()
            self.transport.post_observation(self.node_id, observation)
        return resp

    def close(self) -> None:
        self.state.close()
