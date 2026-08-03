"""Kubernetes-integrated mode (DESIGN.md 6.3): the agent reports engines the
cluster scheduled on this node, read-only, and never launches one itself.
"""

from __future__ import annotations

import pytest
from fake_control_plane import FakeControlPlane, Token

from meridian_node.agent import Agent
from meridian_node.config import Config, KubernetesConfig
from meridian_node.k8s import pod_to_engine

CFG = KubernetesConfig()


# --- pod -> engine mapping ----------------------------------------------
def test_ready_pod_maps_to_routable_endpoint():
    pod = {"name": "engine-1", "phase": "Running", "ready": True, "pod_ip": "10.0.0.5",
           "annotations": {CFG.engine_port_annotation: "8000", CFG.model_annotation: "demo/model"}}
    e = pod_to_engine(pod, CFG)
    assert e["phase"] == "Ready"
    assert e["endpoint"] == "https://10.0.0.5:8000"
    assert e["model"] == "demo/model"


def test_pending_pod_has_no_endpoint():
    pod = {"name": "engine-1", "phase": "Pending", "ready": False, "pod_ip": None, "annotations": {}}
    e = pod_to_engine(pod, CFG)
    assert e["phase"] == "Warming" and e["endpoint"] == ""


def test_failed_pod_maps_to_failed():
    pod = {"name": "engine-1", "phase": "Failed", "ready": False, "pod_ip": None, "annotations": {}}
    assert pod_to_engine(pod, CFG)["phase"] == "Failed"


# --- agent behaviour ----------------------------------------------------
class _FakeObserver:
    def __init__(self, engines):
        self._engines = engines
        self.calls = 0

    def observe(self):
        self.calls += 1
        return self._engines


def _k8s_agent(tmp_path, clock):
    fake = FakeControlPlane({"auto": Token(auto_approve=True)}, now=clock)
    cfg = Config(control_plane_url="fake://cp", state_dir=tmp_path, mode="kubernetes-integrated")
    agent = Agent(cfg, fake.presenting("auto"), now=clock)
    agent.ensure_enrolled()
    agent.establish_session()
    return agent, fake


def test_tick_reports_observed_engines_and_never_launches(tmp_path, clock_factory):
    agent, fake = _k8s_agent(tmp_path, clock_factory())
    observer = _FakeObserver([{"engine_id": "pod-a", "phase": "Ready",
                               "endpoint": "https://10.0.0.9:8000", "model": "m1"}])
    agent.attach_observer(observer)

    agent.tick()

    assert agent.reconciler is None  # kubernetes-integrated never launches
    assert observer.calls == 1
    obs = fake.observations[agent.node_id]
    assert obs["engines"][0]["engine_id"] == "pod-a"
    assert obs["node_id"] == agent.node_id


def test_attach_observer_rejects_wrong_mode(tmp_path, clock_factory):
    clock = clock_factory()
    fake = FakeControlPlane({"auto": Token(auto_approve=True)}, now=clock)
    cfg = Config(control_plane_url="fake://cp", state_dir=tmp_path, mode="observe-only")
    agent = Agent(cfg, fake.presenting("auto"), now=clock)
    with pytest.raises(ValueError, match="kubernetes-integrated"):
        agent.attach_observer(object())
