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

The agent is implemented through Phases 0–4 (Phase 5 in progress), with adapter
seams (fakes in tests) at the hardware and network boundaries (NVML, Docker, mTLS
HTTP) as the testing strategy prescribes. All four modes ship (observe-only,
managed-host, kubernetes-integrated, air-gapped) and seven engine drivers.

| Area | State |
|---|---|
| Contracts (`contracts/v1`) | JSON Schemas + golden fixtures, contract-tested |
| Identity, durable state, enrollment, heartbeat, lease | Implemented + tested |
| Fencing / replay / restart recovery | Implemented + tested |
| Artifact trust chain (signed manifest, digest, atomic promote, LRU) | Implemented + tested |
| Engine drivers (vLLM, Ollama, generic-OpenAI, TGI, SGLang, llama.cpp, LMDeploy) | Implemented + tested |
| Modes: observe-only, managed-host, kubernetes-integrated (read-only), air-gapped | Implemented + tested |
| Placement: MIG parent/child + VRAM enforced at launch; control-side capacity-aware node selection | Implemented + tested |
| Supervisor, operation journal, reservation, reconciliation | Implemented + tested |
| Root helper privilege boundary | Implemented + tested (protocol); Docker backend is host-tested |
| **mTLS enforced end to end** (client cert presented + SAN bound to path) | Implemented + tested |
| **Certificate rotation** (renew before expiry) | Implemented + tested |
| **Allocatable capacity reporting** (VRAM + ports per observation) | Implemented + tested |
| **Gateway consumes serving projection** (managed engines become routable backends) | Implemented + tested |
| **Air-gapped mode** (signed offline bundles, inbox/outbox) | Implemented + tested |
| NVML provider, metrics | Implemented (HTTP mapping + NVML degrade tested) |

The production central control plane exists as **`meridian-control`** in the
[Meridian repo](https://github.com/IMV-IN/Meridian): FastAPI + SQLAlchemy
(SQLite/Postgres) with a built-in Ed25519 CA. The full `node ↔ control ↔ gateway`
path is verified end to end with mTLS enforced, with measured numbers — see
`Meridian/scripts/verify_connection.py` and DESIGN.md §20.

A runnable dev/reference control plane (`meridian-node-devserver`) implements the
v1 wire protocol so the real HTTP transport and the managed loop can be driven
end to end (`tests/test_live_server.py`) without the production service.

Remaining frontier: Phase-5 placement breadth — artifact locality, queue-depth-
aware assignment, and multi-GPU/NVLink affinity. (MIG/VRAM enforcement at launch
and capacity-aware node selection are done.)

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[http,dev]"
pytest -q                    # 92 tests
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

### Air-gapped mode

No control channel: desired state arrives as a **signed bundle** in an inbox and
observations spool to an outbox (DESIGN.md §6.4). The bundle signature (a
dedicated `air_gap.authority_keys`) is the offline trust anchor. Sign a bundle
with an authority key, then run in `mode: air-gapped`:

```bash
meridian-node make-bundle --snapshot desired.json --key authority.pem \
  --key-id auth1 --out inbox/desired/bundle.json
meridian-node --config deploy/config.air-gapped.yaml run
```

See [`docs/INSTALL.md`](docs/INSTALL.md), [`docs/SECURITY.md`](docs/SECURITY.md),
and [`docs/ENGINE_DRIVERS.md`](docs/ENGINE_DRIVERS.md).
