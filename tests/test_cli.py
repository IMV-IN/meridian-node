"""CLI surface: fail-closed config and the managed-gate readout."""

from __future__ import annotations

from meridian_node.cli import main
from meridian_node.config import load_config


def _cfg(tmp_path, text):
    p = tmp_path / "config.yaml"
    p.write_text(text)
    return str(p)


def test_check_config_ok(tmp_path, capsys):
    rc = main(["--config", _cfg(tmp_path, "control_plane_url: https://cp\n"), "check-config"])
    assert rc == 0
    assert "managed=False" in capsys.readouterr().out


def test_check_config_fails_closed(tmp_path, capsys):
    rc = main(["--config", _cfg(tmp_path, "mode: observe-only\n"), "check-config"])  # missing url
    assert rc == 2


def test_managed_gate_requires_both_mode_and_policy(tmp_path):
    # managed-host mode alone is not enough; the local policy switch must also be on.
    cfg = load_config(_cfg(tmp_path, "control_plane_url: https://cp\nmode: managed-host\n"))
    assert cfg.managed_allowed() is False

    text = "control_plane_url: https://cp\nmode: managed-host\npolicy:\n  management_enabled: true\n"
    cfg2 = load_config(_cfg(tmp_path, text))
    assert cfg2.managed_allowed() is True


def test_policy_bad_port_range_rejected(tmp_path):
    text = "control_plane_url: https://cp\npolicy:\n  port_range: [9000, 8000]\n"
    import pytest

    from meridian_node.config import ConfigError

    with pytest.raises(ConfigError):
        load_config(_cfg(tmp_path, text))
