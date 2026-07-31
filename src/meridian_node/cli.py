"""Command-line entrypoint.

Phase 0 exposes config validation and a status readout. `run` is intentionally
not wired to a live control plane yet: the real mTLS HTTP transport is the next
step, and engine control does not exist until fencing/lease tests pass
(DESIGN.md 24).
"""

from __future__ import annotations

import argparse
import sys

from .config import ConfigError, load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="meridian-node")
    parser.add_argument("--config", required=True, help="path to agent config YAML")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config", help="validate configuration and exit")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        print(f"ok: mode={config.mode} control_plane={config.control_plane_url}")
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
