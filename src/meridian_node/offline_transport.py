"""Offline transport for air-gapped mode (DESIGN.md 6.4).

Implements the `Transport` contract without a network: desired state is read from
signed bundles in an inbox directory, observations are spooled to an outbox for
the operator to collect, and the lease/heartbeat/session become local operations.
Everything downstream (reconciler, supervisor, journal) is unchanged.

Inbox layout (operator-populated, via removable media):
    inbox/desired/*.json        signed desired-state bundles (highest gen wins)
    inbox/manifests/<digest>.json   signed artifact manifests
    inbox/artifacts/                artifact file tree (LocalFetcher source root)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from .airgap import BundleError, VerifiedBundle, verify_desired_bundle
from .control import ControlError

logger = logging.getLogger("meridian_node.airgap")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class OfflineTransport:
    def __init__(
        self,
        node_id: str,
        inbox_dir: Path,
        outbox_dir: Path,
        authority_keys: dict[str, str],
        state_dir: Path,
        lease_ttl_seconds: int = 30,
        now: Callable[[], datetime] = _utcnow,
    ):
        self.node_id = node_id
        self.inbox_dir = Path(inbox_dir)
        self.outbox_dir = Path(outbox_dir)
        self.authority_keys = authority_keys
        self._epoch_file = Path(state_dir) / "airgap_epoch"
        self.lease_ttl = timedelta(seconds=lease_ttl_seconds)
        self.now = now

    # --- bundle reading -------------------------------------------------
    def _latest_bundle(self) -> Optional[VerifiedBundle]:
        """Highest-generation *verified* bundle in the inbox. Invalid or
        unsigned bundles are skipped (logged), never applied."""
        desired = self.inbox_dir / "desired"
        best: Optional[VerifiedBundle] = None
        for path in sorted(desired.glob("*.json")) if desired.is_dir() else []:
            try:
                raw = json.loads(path.read_text())
                bundle = verify_desired_bundle(raw, self.authority_keys)
            except (BundleError, ValueError, OSError) as e:
                logger.warning("ignoring invalid desired bundle %s: %s", path.name, e)
                continue
            if bundle.snapshot.get("node_id") != self.node_id:
                continue  # a bundle addressed to another node
            if best is None or bundle.generation > best.generation:
                best = bundle
        return best

    # --- Transport contract --------------------------------------------
    def enroll(self, request: dict) -> dict:
        # Identity is provisioned via config; no network, no cert.
        return {"status": "approved", "node_id": self.node_id, "certificate": "", "trust_bundle": ""}

    def get_claim(self, claim_id: str, signed_nonce_provider) -> dict:  # pragma: no cover - unused offline
        return {"status": "approved", "node_id": self.node_id, "certificate": "", "trust_bundle": ""}

    def establish_session(self, node_id: str, request: dict) -> dict:
        # Monotonic local epoch (restart-safe) so the root helper has a fence.
        # No live-takeover concern with a single offline agent.
        try:
            current = int(self._epoch_file.read_text())
        except (OSError, ValueError):
            current = 0
        epoch = current + 1
        self._epoch_file.parent.mkdir(parents=True, exist_ok=True)
        self._epoch_file.write_text(str(epoch))
        return {"fencing_epoch": epoch, "lease_expires_at": _iso(self.now() + self.lease_ttl)}

    def heartbeat(self, node_id: str, request: dict) -> dict:
        bundle = self._latest_bundle()
        return {
            "server_time": _iso(self.now()),
            "accepted_sequence": request["sequence"],
            "fencing_epoch": request["fencing_epoch"],
            "lease_expires_at": _iso(self.now() + self.lease_ttl),
            "desired_generation": bundle.generation if bundle else 0,
            "admission_projection_revision": 0,
            "stop_authorized_engine_ids": bundle.stop_authorized_engine_ids if bundle else [],
        }

    def fetch_desired_state(self, node_id: str, generation: int) -> dict:
        bundle = self._latest_bundle()
        if bundle is None:
            raise ControlError("GENERATION_NOT_FOUND", "no valid desired-state bundle in inbox")
        return bundle.snapshot

    def post_observation(self, node_id: str, observation: dict) -> dict:
        self.outbox_dir.mkdir(parents=True, exist_ok=True)
        gen = observation.get("observed_generation", 0)
        seq = observation.get("sequence", 0)
        dest = self.outbox_dir / f"observation-{gen}-{seq}.json"
        dest.write_text(json.dumps(observation, indent=2, sort_keys=True))
        return {"received": True}

    def rotate_certificate(self, node_id: str) -> dict:  # pragma: no cover - no live TLS offline
        return {"certificate": "", "trust_bundle": ""}
