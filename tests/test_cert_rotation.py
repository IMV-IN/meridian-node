"""Automated node-cert rotation (Phase 4): the agent renews its cert once past
a fraction of its lifetime so an enforced-mTLS session never breaks on expiry.
"""

from __future__ import annotations

import datetime as dt

from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID

from meridian_node.agent import Agent
from meridian_node.config import Config

NOW = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def _mint(node_id: str, not_before: dt.datetime, not_after: dt.datetime) -> str:
    ca = Ed25519PrivateKey.generate()
    node = Ed25519PrivateKey.generate()
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, node_id)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ca")]))
        .public_key(node.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier(f"meridian-node://{node_id}")]),
            critical=False,
        )
        .sign(ca, None)
    )
    return cert.public_bytes(Encoding.PEM).decode()


class RotateMock:
    def __init__(self, new_cert: str):
        self.new_cert = new_cert
        self.calls = 0

    def rotate_certificate(self, node_id: str) -> dict:
        self.calls += 1
        return {"certificate": self.new_cert, "trust_bundle": "tb"}


def _agent(tmp_path, cert: str, transport) -> Agent:
    cfg = Config(control_plane_url="http://c", state_dir=tmp_path, mode="observe-only")
    agent = Agent(cfg, transport, now=lambda: NOW)
    agent.node_id = "node_x"
    agent.certificate = cert
    return agent


def test_rotates_when_past_half_life(tmp_path):
    old = _mint("node_x", NOW - dt.timedelta(hours=20), NOW + dt.timedelta(hours=4))  # 4h of 24h left
    new = _mint("node_x", NOW, NOW + dt.timedelta(hours=24))
    mock = RotateMock(new)
    agent = _agent(tmp_path, old, mock)

    assert agent.maybe_rotate_certificate() is True
    assert mock.calls == 1
    assert agent.certificate == new
    assert (tmp_path / "node.crt").read_text() == new  # persisted for restart


def test_no_rotation_with_ample_lifetime(tmp_path):
    fresh = _mint("node_x", NOW - dt.timedelta(hours=1), NOW + dt.timedelta(hours=23))
    mock = RotateMock("unused")
    agent = _agent(tmp_path, fresh, mock)

    assert agent.maybe_rotate_certificate() is False
    assert mock.calls == 0


def test_unparseable_cert_skips(tmp_path):
    mock = RotateMock("unused")
    agent = _agent(tmp_path, "FAKE-CERT-node_x", mock)  # reference/dev fake

    assert agent.maybe_rotate_certificate() is False
    assert mock.calls == 0
