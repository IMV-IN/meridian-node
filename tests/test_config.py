"""Config fails closed (DESIGN.md 23, acceptance checklist item 1)."""

from __future__ import annotations

import pytest

from meridian_node.config import ConfigError, load_config


def _write(tmp_path, text):
    p = tmp_path / "config.yaml"
    p.write_text(text)
    return p


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")


def test_empty_file(tmp_path):
    with pytest.raises(ConfigError, match="empty"):
        load_config(_write(tmp_path, "   \n"))


def test_not_a_mapping(tmp_path):
    with pytest.raises(ConfigError, match="mapping"):
        load_config(_write(tmp_path, "- just\n- a\n- list\n"))


def test_missing_required_field(tmp_path):
    with pytest.raises(ConfigError, match="invalid config"):
        load_config(_write(tmp_path, "mode: observe-only\n"))


def test_unknown_field_rejected(tmp_path):
    text = "control_plane_url: https://cp\nsurprise: true\n"
    with pytest.raises(ConfigError):
        load_config(_write(tmp_path, text))


def test_bad_mode_rejected(tmp_path):
    text = "control_plane_url: https://cp\nmode: god-mode\n"
    with pytest.raises(ConfigError, match="mode"):
        load_config(_write(tmp_path, text))


def test_valid_config_defaults_to_observe_only(tmp_path):
    cfg = load_config(_write(tmp_path, "control_plane_url: https://cp\n"))
    assert cfg.mode == "observe-only"
    assert cfg.heartbeat_interval_seconds == 10
