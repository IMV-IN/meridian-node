# Changelog

## 0.1.0 (unreleased)

- **Python 3.14 everywhere.** The agent image moves to
  `python:3.14-slim-trixie` and `.python-version` pins 3.14 for local
  development. Supported range is now **3.11 – 3.14**
  (`requires-python >= 3.11`, all tested in CI); 3.10 is end-of-life upstream.

First implementation of the node agent, built against `DESIGN.md`.

### Contracts and safety foundation (Phase 0)
- `contracts/v1` JSON Schemas (enrollment, heartbeat, desired-state,
  observed-state, common) with golden fixtures and contract tests.
- Fail-closed config loader; Ed25519 node identity; SQLite (WAL) durable state.
- Enrollment (approval-gated with `auto_approve`), fenced session establishment
  with the lease-expiry takeover precondition, and heartbeat with a monotonic
  sequence that continues across restart.
- In-process fake control plane enforcing single-use tokens, epoch fencing, and
  sequence monotonicity.

### Engine lifecycle and reconciliation (Phases 1–3)
- Engine assignment state machine with enforced transition legality.
- Artifact trust chain: signed-manifest verification, per-file digest/size
  checks, path-traversal/symlink rejection, staged atomic promotion, pinned LRU.
- Engine drivers (vLLM, generic OpenAI) with typed parameter allow-lists and
  launch-request construction — no free-form commands.
- Operation journal with crash-recovery states; local reservation transaction.
- Supervisor: journaled launch, restart backoff, quarantine, drain that waits
  for central stop authorization, adoption on exact match, orphan safety.
- Reconciler: snapshot application, deletion via drain, generation monotonicity,
  observed-state emission. Managed agent `tick()` loop.

### Privilege boundary and operations
- Root helper: epoch fencing, per-request policy allow-lists, replay protection,
  idempotent journal; Unix-socket daemon, Docker CLI backend, in-memory fake.
- Local root-owned policy with the `management_enabled` kill switch.
- Real mTLS HTTP transport with error-envelope mapping; HTTP artifact transfer
  with source-host allow-listing; read-only NVML inventory provider; bounded
  metrics; CLI (`check-config`, `run`, `helper`).
- Deploy artifacts (systemd units, Dockerfile, example configs) and docs
  (INSTALL, SECURITY, ENGINE_DRIVERS).

### Reference control plane
- `refcontrol.ControlCore`: the authoritative in-memory server-side safety rules
  (single-use tokens, epoch fencing, lease-expiry takeover, sequence
  monotonicity), shared by the test fake and the dev server so there is one
  implementation, not two.
- `meridian-node-devserver`: a stdlib HTTP server exposing the v1 contract for
  local dev and CI. Drives the real HttpTransport and managed loop end to end
  (`tests/test_live_server.py`). Not the production control plane.
- Enrollment now persists a pending claim and resumes it (across restart)
  instead of re-spending a used token.

### Production frontier (P0–P4)
- **mTLS enforced end to end (P0):** the agent presents its client certificate on
  every post-enrollment call; the control plane's `verify_node_identity` binds the
  cert SAN `node_id` to the path (no-cert → `NODE_NOT_AUTHORIZED`). Default-off
  `require_mtls`, enabled behind the TLS-terminating edge.
- **Gateway consumes the serving projection (P1):** nodes report the served model
  per Ready engine; the Meridian gateway registers routable managed engines as
  dynamic backends.
- **Ollama driver (P2)** on the existing typed driver seam.
- **Allocatable capacity reporting (P3):** authoritative VRAM + free ports per
  observation, for central placement.
- **Certificate rotation (Phase 4):** the agent renews before expiry via
  `POST /control/v1/nodes/{id}/certificate`.
- **Air-gapped mode (Phase 4):** signed desired-state bundles read from an inbox,
  observations spooled to an outbox, local manifest/fetch through the unchanged
  verification path; `make-bundle` CLI. The bundle signature (dedicated authority
  key) is the offline trust anchor.
- **End-to-end connection verified with numbers:** the full `node ↔ control ↔
  gateway` path over a real socket with mTLS enforced
  (`Meridian/scripts/verify_connection.py`, `test_cross_repo_gateway.py`).

### Phase 4 complete + Phase 5 (in progress)
- **Engine breadth:** drivers for all OpenAI-compatible gateway engines — TGI,
  SGLang, llama.cpp, LMDeploy (shared `_OpenAIServerDriver`), joining
  vLLM/Ollama/generic-OpenAI.
- **kubernetes-integrated mode (Phase 4 complete):** read-only
  `KubernetesEngineObserver` reports cluster-scheduled engines on this node;
  structurally cannot launch.
- **Placement (Phase 5):** MIG parent/child exclusivity + VRAM enforced at launch
  in the reservation ledger; control-side `select_placement` capacity-aware node
  selection (`POST /admin/place`).

### Production hardening (completes Phases 0–5)
- **All OpenAI-compatible engine drivers:** TGI, SGLang, llama.cpp, LMDeploy.
- **Placement (Phase 5):** node reports held artifacts, engine load, and NVLink
  groups; control-side `select_placement` chooses by artifact locality → load →
  headroom, with NVLink-group affinity for multi-GPU requests.
- **Revocation hardening:** enforced on every post-enrollment call; `GET /admin/crl`.
- **Alembic migrations** manage the production schema (`meridian-control migrate`).
- **CI:** GitHub Actions for both repos, including a control-plane job that runs
  the cross-repo node↔control tests.
- **GPU host-test scaffolding** gated on `MERIDIAN_GPU_HOST`.

- **Queue-depth-aware placement:** the node scrapes live queue depth per Ready
  engine; control-side `select_placement` uses it as the load metric.
- **DB connection pooling:** non-SQLite engines get a sized, pre-pinged,
  recycling pool (`MERIDIAN_CONTROL_DB_POOL_SIZE` / `_MAX_OVERFLOW`).

- **GPU host tests executed on real hardware (RTX 4060, driver 580):** the `gpu`
  extra (`nvidia-ml-py`) + gated `test_host_gpu.py` cover the real `NvmlProvider`
  and the real inventory flowing through a real agent → HTTP → control heartbeat.

### Not yet included (needs a measured load profile)
- Load-driven batched observation writes beyond ~500 nodes (Section 10.7);
  deliberately deferred — it trades read-freshness for throughput and should follow
  a measured load profile. Connection pooling is the first lever and is in place.

Tests: 95 passing (+ 3 GPU host tests that pass with `MERIDIAN_GPU_HOST=1` on an
RTX 4060, + 1 external compat skipped); `ruff` and `mypy` clean. Cross-repo:
gateway 540, control + cross-repo 41.
