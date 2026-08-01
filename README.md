# Meridian Node Agent

`meridian-node` is the host-local agent for GPU inference servers. It connects
to central Meridian, reports GPU/model/engine state, reconciles approved desired
state, manages verified artifacts, and supervises inference engines on a GPU
host.

Read [`DESIGN.md`](DESIGN.md) for the full contract: repository boundary,
first-release deployment profile, control-plane protocol, identity and fencing
model, artifact trust chain, engine lifecycle, drain semantics, security policy,
testing strategy, and delivery gates.

The initial managed profile is deliberately narrow: Linux, systemd, Docker
Engine, NVIDIA GPUs, pinned vLLM OCI images, PostgreSQL-backed central control
state, and private gateway-to-engine networking. Observe-only mode can support
additional environments without granting them lifecycle-control privileges.

## Status

The agent is implemented through the reconciliation and managed-engine layers,
with adapter seams (fakes in tests) at the hardware and network boundaries
(NVML, Docker, mTLS HTTP) as the testing strategy prescribes.

| Area | State |
|---|---|
| Contracts (`contracts/v1`) | JSON Schemas + golden fixtures, contract-tested |
| Identity, durable state, enrollment, heartbeat, lease | Implemented + tested |
| Fencing / replay / restart recovery | Implemented + tested |
| Artifact trust chain (signed manifest, digest, atomic promote, LRU) | Implemented + tested |
| Engine drivers (vLLM, generic OpenAI) | Implemented + tested |
| Supervisor, operation journal, reservation, reconciliation | Implemented + tested |
| Root helper privilege boundary | Implemented + tested (protocol); Docker backend is host-tested |
| Real mTLS HTTP transport, NVML provider, metrics | Implemented (HTTP mapping + NVML degrade tested) |

A runnable dev/reference control plane (`meridian-node-devserver`) implements
the v1 wire protocol so the real HTTP transport and the managed loop can be
driven end to end (`tests/test_live_server.py`). It is a development/CI tool,
not the production `meridian-control` service (which belongs in the central
Meridian repo, with Postgres and a real CA).

Not yet wired: the production central Meridian control plane (compat target in
`tests/test_compat.py`), Kubernetes-integrated and air-gapped modes (Phase 4),
and placement/capacity (Phase 5).

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[http,dev]"
pytest -q                    # 66 tests
ruff check src tests && mypy src/meridian_node

# Validate a config and run one observe-only cycle against a control plane:
meridian-node --config deploy/config.observe-only.yaml check-config
export MERIDIAN_NODE_TOKEN=<one-time-token>
meridian-node --config deploy/config.observe-only.yaml run --once
```

Drive it against a local control plane end to end:

```bash
# Terminal 1: reference control plane (dev/CI only, not production)
meridian-node-devserver --port 8443 --token dev-token:auto

# Terminal 2: point the agent at it
export MERIDIAN_NODE_TOKEN=dev-token
meridian-node --config deploy/config.observe-only.yaml run --once   # control_plane_url: http://127.0.0.1:8443
```

See [`docs/INSTALL.md`](docs/INSTALL.md), [`docs/SECURITY.md`](docs/SECURITY.md),
and [`docs/ENGINE_DRIVERS.md`](docs/ENGINE_DRIVERS.md).
