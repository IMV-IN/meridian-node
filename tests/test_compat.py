"""Cross-repository compatibility target (DESIGN.md 20, 24 step 6).

Skipped unless MERIDIAN_COMPAT_URL points at a running central Meridian control
API. When set, it exercises the real enrollment/heartbeat contract end to end
using the same HttpTransport the agent uses, so protocol drift between the two
repositories is caught in CI. A one-time token must be supplied in
MERIDIAN_COMPAT_TOKEN.
"""

from __future__ import annotations

import os

import pytest

COMPAT_URL = os.environ.get("MERIDIAN_COMPAT_URL")
COMPAT_TOKEN = os.environ.get("MERIDIAN_COMPAT_TOKEN")

pytestmark = pytest.mark.skipif(not COMPAT_URL, reason="set MERIDIAN_COMPAT_URL to run compatibility tests")


def test_enroll_and_heartbeat_against_real_control_plane(tmp_path):
    from meridian_node.agent import Agent
    from meridian_node.config import Config
    from meridian_node.http_transport import HttpTransport

    transport = HttpTransport(COMPAT_URL, enrollment_token=COMPAT_TOKEN)
    cfg = Config(control_plane_url=COMPAT_URL, state_dir=tmp_path, mode="observe-only")
    agent = Agent(cfg, transport)

    node_id = agent.ensure_enrolled(poll_interval=1.0, max_polls=60)
    assert node_id
    agent.establish_session()
    resp = agent.heartbeat_once()
    assert "lease_expires_at" in resp
    assert resp["desired_generation"] >= 0
