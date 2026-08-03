"""Assemble a live agent from configuration (operator entrypoint plumbing).

Kept out of agent.py so the core agent stays transport- and hardware-agnostic
and fully testable with fakes. This module is where real httpx/NVML/Docker
adapters are chosen based on config and local policy.
"""

from __future__ import annotations

import base64
from pathlib import Path

from .agent import Agent
from .config import Config
from .reconcile import Reconciler


def _decode_keys(trusted: dict[str, str]) -> dict[str, bytes]:
    return {kid: base64.urlsafe_b64decode(v + "=" * (-len(v) % 4)) for kid, v in trusted.items()}


def build_agent(config: Config, enrollment_token: str | None, now=None) -> Agent:
    kwargs = {}
    if now is not None:
        kwargs["now"] = now

    from .control import Transport

    transport: Transport
    if config.mode == "air-gapped":
        from .offline_transport import OfflineTransport

        ag = config.air_gap
        transport = OfflineTransport(
            node_id=ag.node_id,
            inbox_dir=ag.inbox_dir,
            outbox_dir=ag.outbox_dir,
            authority_keys=ag.authority_keys,
            state_dir=config.state_dir,
            lease_ttl_seconds=config.lease_ttl_seconds,
            **({"now": now} if now is not None else {}),
        )
    else:
        from .http_transport import HttpTransport

        transport = HttpTransport(config.control_plane_url, enrollment_token=enrollment_token)
    return Agent(config, transport, **kwargs)


def upgrade_transport(config: Config, agent: Agent) -> None:
    """After enrollment, swap the token transport for one that presents the node
    mTLS client certificate on every call (DESIGN.md 15.6). No-op if the cert
    material is absent (e.g. an older enrollment or a test fake)."""
    from .http_transport import HttpTransport

    state_dir = Path(config.state_dir)
    crt, key = state_dir / "node.crt", state_dir / "node.key"
    if not (crt.exists() and key.exists()):
        return
    # ponytail: verify defaults to system trust; add a serving-CA bundle option
    # only if the control plane serves TLS from the built-in node CA.
    agent.transport = HttpTransport(
        config.control_plane_url,
        cert=(str(crt), str(key)),
        client_cert_pem=crt.read_text(),
    )


def build_managed_reconciler(config: Config, agent: Agent, now=None):
    """Construct the supervisor + reconciler for managed-host mode. Raises if the
    local policy has not enabled management (DESIGN.md 15.1)."""
    if not config.managed_allowed():
        raise ValueError("managed reconciliation requires managed-host mode and policy.management_enabled")
    if agent.node_id is None:
        raise ValueError("agent must be enrolled before building the reconciler")
    node_id: str = agent.node_id

    from .artifacts import ArtifactStore, Fetcher
    from .inventory import NvmlProvider
    from .journal import Journal
    from .reservation import ReservationLedger
    from .supervisor import EngineSupervisor, ManifestSource

    policy = config.policy
    state_dir = Path(config.state_dir)
    store = ArtifactStore(state_dir / "artifacts")
    journal = Journal(state_dir / "journal.db")
    # MIG topology for parent/child exclusivity at launch (Phase 5). Best-effort:
    # a physical-only host (or missing NVML) yields an empty topology and the
    # ledger falls back to VRAM/port checks only.
    provider = NvmlProvider()
    try:
        topology = {g["device_id"]: g.get("parent_device_id") for g in provider.gpus()}
    except Exception:
        topology = {}
    ledger = ReservationLedger(
        device_capacity=policy.device_capacity_bytes,
        port_range=range(policy.port_range[0], policy.port_range[1]),
        topology=topology,
    )
    if config.mode == "air-gapped":
        from .local_artifacts import LocalFetcher, LocalManifestSource

        fetcher: Fetcher = LocalFetcher(Path(config.air_gap.inbox_dir) / "artifacts")
        manifests: ManifestSource = LocalManifestSource(config.air_gap.inbox_dir)
    else:
        from .transfer import HttpFetcher, HttpManifestSource

        fetcher = HttpFetcher(policy.allowed_source_hosts)
        manifests = HttpManifestSource(config.control_plane_url, policy.allowed_source_hosts)

    sup_kwargs = {}
    if now is not None:
        sup_kwargs["now"] = now
    supervisor = EngineSupervisor(
        node_id=node_id,
        runtime=_build_runtime(config, agent),
        artifacts=store,
        manifests=manifests,
        prober=_build_prober(),
        journal=journal,
        reservations=ledger,
        trusted_keys=_decode_keys(policy.trusted_signing_keys),
        fetcher=fetcher,
        engine_host=policy.engine_host,
        **sup_kwargs,
    )
    reconciler = Reconciler(node_id, supervisor, agent.state, **sup_kwargs)
    agent.attach_reconciler(reconciler)
    # NVML provider is the source of truth for local inventory in managed mode.
    agent.inventory = provider
    return reconciler


def _build_runtime(config: Config, agent: Agent):
    """Docker-via-root-helper runtime. Requires an established session so the
    fencing epoch is known (DESIGN.md 14.1)."""
    if agent.fencing_epoch is None:
        raise ValueError("establish a session before building the managed runtime")
    from .helper import unix_socket_send
    from .runtime import HelperRuntime

    send = unix_socket_send(config.policy.helper_socket_path)
    return HelperRuntime(send, session_id=agent.session_id, fencing_epoch=agent.fencing_epoch)


def _build_prober():
    import httpx

    from .drivers import ReadinessSpec

    class HttpProber:
        def probe(self, host: str, port: int, spec: ReadinessSpec) -> bool:
            try:
                r = httpx.get(f"http://{host}:{port}{spec.path}", timeout=2.0)
            except httpx.HTTPError:
                return False
            return r.status_code == 200

    return HttpProber()
