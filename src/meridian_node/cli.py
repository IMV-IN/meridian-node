"""Command-line entrypoint.

- check-config: validate configuration and exit (fails closed).
- run: enroll, establish a session, and heartbeat/reconcile in a loop.
- helper: run the root helper daemon (managed-host privilege boundary).

The engine-control path is refused unless both managed-host mode and the local
policy switch are enabled (DESIGN.md 15.1).
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from .config import Config, ConfigError, load_config
from .control import ControlError


def _run_loop(config: Config, once: bool, now=None) -> int:
    from .wiring import build_agent, build_managed_reconciler, upgrade_transport

    token = os.environ.get("MERIDIAN_NODE_TOKEN")
    agent = build_agent(config, token, now=now)
    agent.ensure_enrolled()
    upgrade_transport(config, agent)  # present the node mTLS cert for the session
    agent.establish_session()

    if config.managed_allowed():
        build_managed_reconciler(config, agent, now=now)
        print("running in managed-host mode", file=sys.stderr)
    else:
        print(f"running in {config.mode} mode (no engine control)", file=sys.stderr)

    backoff = 1.0
    while True:
        try:
            agent.tick()
            backoff = 1.0
        except ControlError as e:
            if e.code == "STALE_AGENT_SESSION":
                print("session fenced; re-establishing", file=sys.stderr)
                agent.establish_session()
            elif e.retryable:
                time.sleep(min(backoff, 60.0))
                backoff *= 2
            else:
                print(f"fatal control error: {e.code}", file=sys.stderr)
                return 1
        if once:
            return 0
        time.sleep(config.heartbeat_interval_seconds)


def _run_helper(config: Config) -> int:
    from .helper import DockerContainerBackend, HelperPolicy, HelperServer, serve_unix_socket
    from .journal import Journal

    p = config.policy
    if not p.management_enabled:
        print("error: policy.management_enabled is false; helper refuses to start", file=sys.stderr)
        return 2
    policy = HelperPolicy(
        allowed_image_digests=frozenset(p.allowed_image_digests),
        allowed_devices=frozenset(p.device_capacity_bytes),
        port_start=p.port_range[0], port_stop=p.port_range[1],
        allowed_mount_roots=(str(config.state_dir),),
    )
    backend = DockerContainerBackend(image_repos={})  # repos supplied by deployment
    server = HelperServer(backend, Journal(config.state_dir / "helper.db"), policy)
    print(f"helper listening on {p.helper_socket_path}", file=sys.stderr)
    serve_unix_socket(server, p.helper_socket_path)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="meridian-node")
    parser.add_argument("--config", required=True, help="path to agent config YAML")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config", help="validate configuration and exit")
    run = sub.add_parser("run", help="enroll and run the agent loop")
    run.add_argument("--once", action="store_true", help="run a single cycle and exit")
    sub.add_parser("helper", help="run the root helper daemon")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        print(f"ok: mode={config.mode} managed={config.managed_allowed()} control={config.control_plane_url}")
        return 0
    if args.command == "run":
        return _run_loop(config, once=args.once)
    if args.command == "helper":
        return _run_helper(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
