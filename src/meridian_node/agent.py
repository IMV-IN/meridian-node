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

    # --- enrollment -----------------------------------------------------
    def ensure_enrolled(self, poll_interval: float = 0.0, max_polls: int = 100) -> str:
        """Enroll if no durable identity exists. Handles pending approval."""
        if self.node_id:
            return self.node_id

        request = {
            "agent_version": AGENT_VERSION,
            "protocol_versions": [PROTOCOL_VERSION],
            "node_public_key": self.identity.public_key_b64url(),
            "host_claims": {"hostname": self.config.display_name or "", "platform": "linux"},
            "host_boot_id": self.host_boot_id,
            "agent_session_id": self.session_id,
        }
        resp = self.transport.enroll(request)

        if resp["status"] == "pending":
            resp = self._await_claim(resp["claim_id"], poll_interval, max_polls)

        self.node_id = resp["node_id"]
        self.certificate = resp["certificate"]
        self.state.save_identity(self.node_id, self.certificate, _iso(self.now()))
        return self.node_id

    def _await_claim(self, claim_id: str, poll_interval: float, max_polls: int) -> dict:
        for _ in range(max_polls):
            resp = self.transport.get_claim(claim_id, self.identity.sign_b64url)
            if resp["status"] == "approved":
                return resp
            if resp["status"] == "denied":
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
        self.state.set_accepted_generation(resp["desired_generation"])
        self.lease_expires_at = resp["lease_expires_at"]
        return resp

    def close(self) -> None:
        self.state.close()
