"""Kubernetes-integrated mode: read-only engine observation (DESIGN.md 6.3).

In this mode the central Meridian controller creates native Kubernetes workloads;
the agent reports node/GPU inventory and *observes* the engines the cluster
scheduled on this node so central Meridian sees their serving state. It NEVER
launches an engine — there is no runtime, helper, or reconciler here, so it
cannot start a second copy of a Kubernetes-managed engine.
"""

from __future__ import annotations

import os
from typing import Optional, Protocol

from .config import KubernetesConfig


class EngineObserver(Protocol):
    def observe(self) -> list[dict]:
        """Return engine observations for engines running on this node."""
        ...


def pod_to_engine(pod: dict, cfg: KubernetesConfig) -> dict:
    """Map a normalized pod to an engine observation. Ready only when the pod is
    Running + Ready and exposes an endpoint; otherwise no endpoint is reported so
    it is never treated as routable."""
    ann = pod.get("annotations", {}) or {}
    port = ann.get(cfg.engine_port_annotation)
    ready = pod.get("phase") == "Running" and bool(pod.get("ready"))
    if ready:
        phase = "Ready"
    elif pod.get("phase") in ("Running", "Pending"):
        phase = "Warming"
    else:
        phase = "Failed"
    endpoint = ""
    if ready and pod.get("pod_ip") and port:
        endpoint = f"https://{pod['pod_ip']}:{port}"
    return {
        "engine_id": pod["name"],
        "phase": phase,
        "restart_count": 0,
        "loaded_artifact_digest": None,
        "endpoint": endpoint,
        "model": ann.get(cfg.model_annotation, ""),
        "conditions": [],
    }


class KubernetesEngineObserver:
    """Lists Meridian-labeled pods on this node via the Kubernetes API and maps
    them to engine observations. Read-only. Requires the `k8s` extra; the API
    client is injected in tests."""

    def __init__(self, cfg: KubernetesConfig, api=None):
        self._cfg = cfg
        self._node = cfg.node_name or os.environ.get("MERIDIAN_K8S_NODE_NAME", "")
        self._api = api

    def _client(self):
        if self._api is None:  # pragma: no cover - requires a cluster
            from kubernetes import client, config  # type: ignore[import-not-found]

            config.load_incluster_config()
            self._api = client.CoreV1Api()
        return self._api

    def observe(self) -> list[dict]:
        api = self._client()
        field_selector: Optional[str] = f"spec.nodeName={self._node}" if self._node else None
        pods = api.list_namespaced_pod(
            self._cfg.namespace, label_selector=self._cfg.label_selector, field_selector=field_selector
        )
        out = []
        for pod in pods.items:
            conditions = pod.status.conditions or []
            ready = any(c.type == "Ready" and c.status == "True" for c in conditions)
            out.append(pod_to_engine(
                {
                    "name": pod.metadata.name,
                    "phase": pod.status.phase,
                    "ready": ready,
                    "pod_ip": pod.status.pod_ip,
                    "annotations": pod.metadata.annotations or {},
                },
                self._cfg,
            ))
        return out
