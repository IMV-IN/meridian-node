"""Metrics registry enforces bounded, declared labels (DESIGN.md 19)."""

from __future__ import annotations

import pytest

from meridian_node.observability import Metrics


def test_counter_and_gauge_render():
    m = Metrics()
    m.inc("meridian_node_heartbeat_total", result="ok")
    m.inc("meridian_node_heartbeat_total", result="ok")
    m.set("meridian_node_lease_seconds", 12.5, state="valid")
    text = m.render()
    assert 'meridian_node_heartbeat_total{result="ok"} 2' in text
    assert 'meridian_node_lease_seconds{state="valid"} 12.5' in text


def test_undeclared_metric_rejected():
    m = Metrics()
    with pytest.raises(KeyError):
        m.inc("meridian_node_made_up_total", result="ok")


def test_wrong_labels_rejected():
    m = Metrics()
    with pytest.raises(ValueError, match="requires labels"):
        m.inc("meridian_node_heartbeat_total", node_id="node_secret")  # unbounded label refused
