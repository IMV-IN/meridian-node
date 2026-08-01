"""Node cryptographic identity.

An Ed25519 keypair is created once and persisted with owner-only permissions.
The public key is presented at enrollment; the private key never leaves the
host. `node_id` is assigned by the control plane at enrollment and stored
alongside the key. See DESIGN.md sections 8.1 and 15.2.
"""

from __future__ import annotations

import base64
import os
import secrets
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class NodeIdentity:
    """Owns the durable node private key and public-key encoding."""

    def __init__(self, private_key: Ed25519PrivateKey):
        self._private_key = private_key

    @classmethod
    def load_or_create(cls, key_path: str | Path) -> "NodeIdentity":
        p = Path(key_path)
        if p.exists():
            data = p.read_bytes()
            key = serialization.load_pem_private_key(data, password=None)
            if not isinstance(key, Ed25519PrivateKey):
                raise ValueError(f"node key at {p} is not Ed25519")
            return cls(key)
        key = Ed25519PrivateKey.generate()
        pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        p.parent.mkdir(parents=True, exist_ok=True)
        # Write with owner-only permissions from the start; never widen later.
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(pem)
        return cls(key)

    def public_key_b64url(self) -> str:
        raw = self._private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return _b64url(raw)

    def sign_b64url(self, message: bytes) -> str:
        """Sign a challenge nonce to prove key possession during enrollment."""
        return _b64url(self._private_key.sign(message))


def new_session_id() -> str:
    """A fresh agent session ID for every process start. Fences prior processes."""
    return "session_" + secrets.token_hex(16)
