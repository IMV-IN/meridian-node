"""Signed desired-state bundles (DESIGN.md 6.4).

Air-gapped nodes have no authenticated control channel, so the signature on a
desired-state bundle is the trust anchor that decides *what may run* — the
offline equivalent of the mTLS session. Bundles are signed by a dedicated
authority key (config `air_gap.authority_keys`), distinct from the artifact
publisher's `trusted_signing_keys`, so an artifact publisher cannot forge
desired state. Integrity + authenticity only; bundles are not secret.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


class BundleError(Exception):
    """A bundle failed to parse or verify. Fail closed: never apply it."""


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _signing_payload(generation: int, snapshot: dict, stop_ids: list[str]) -> bytes:
    """Canonical bytes covered by the signature. Stable key order + separators so
    the signer and verifier hash identical bytes."""
    body = {
        "generation": generation,
        "snapshot": snapshot,
        "stop_authorized_engine_ids": sorted(stop_ids),
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class VerifiedBundle:
    generation: int
    snapshot: dict
    stop_authorized_engine_ids: list[str]


def sign_desired_bundle(
    snapshot: dict,
    generation: int,
    stop_authorized_engine_ids: list[str],
    key_id: str,
    private_key: Ed25519PrivateKey,
) -> dict:
    """Produce a signed bundle dict (operator/CLI side)."""
    payload = _signing_payload(generation, snapshot, stop_authorized_engine_ids)
    return {
        "generation": generation,
        "snapshot": snapshot,
        "stop_authorized_engine_ids": sorted(stop_authorized_engine_ids),
        "key_id": key_id,
        "signature": _b64url(private_key.sign(payload)),
    }


def verify_desired_bundle(raw: dict, authority_keys: dict[str, str]) -> VerifiedBundle:
    """Verify a bundle against the configured authority keys (agent side)."""
    try:
        generation = int(raw["generation"])
        snapshot = raw["snapshot"]
        stop_ids = list(raw.get("stop_authorized_engine_ids", []))
        key_id = str(raw["key_id"])
        signature = _b64url_decode(str(raw["signature"]))
    except (KeyError, TypeError, ValueError) as e:
        raise BundleError(f"malformed bundle: {e}") from e

    pubkey_b64 = authority_keys.get(key_id)
    if pubkey_b64 is None:
        raise BundleError(f"bundle signed by unknown authority key {key_id!r}")
    try:
        pubkey = Ed25519PublicKey.from_public_bytes(_b64url_decode(pubkey_b64))
        pubkey.verify(signature, _signing_payload(generation, snapshot, stop_ids))
    except (InvalidSignature, ValueError) as e:
        raise BundleError("bundle signature verification failed") from e
    return VerifiedBundle(generation=generation, snapshot=snapshot, stop_authorized_engine_ids=sorted(stop_ids))
