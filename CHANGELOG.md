# Changelog

## 0.1.0 (unreleased)

- **Release workflow.** Pushing a `v*` tag (or a manual dispatch) builds the agent image for linux/amd64 and
  linux/arm64 and pushes `ghcr.io/<owner>/meridian-node:{version,latest}`, and `<DOCKERHUB_USERNAME>/meridian-node`
  too when the Docker Hub secrets are set. The tag must match the `pyproject.toml` version. The wheel and sdist are
  uploaded as a workflow artifact (no GitHub Release, no PyPI). CI now runs with read-only permissions and cancels
  superseded runs. See `docs/RELEASE.md`.

- **Job isolation and the helper boundary (security audit H4, M8; DESIGN.md
  25.3-25.6).** The root helper now sets GPU visibility for every job
  container itself (`NVIDIA_VISIBLE_DEVICES=void` and no `--gpus` for a 0-GPU
  job) and rejects `NVIDIA_*`, `CUDA_VISIBLE_DEVICES` and `CUDA_MPS_*` in job
  env, in the model and again in the helper. The helper accepts only known job
  fields and re-validates each with allow-list patterns and size bounds. Job
  env goes through a 0600 env file instead of `-e` on the command line. Job
  containers get a memory cap (new `policy.job_memory_default_bytes`, 32 GiB,
  and `policy.job_memory_max_bytes`, 256 GiB), swap capped to the same value,
  an OOM score that sacrifices jobs before the agent, an optional CPU limit
  (`policy.job_cpus`, default none), and the `meridian-jobs` bridge with
  inter-container traffic disabled. Published job ports bind to
  `policy.job_bind_address` (default: `engine_host`) instead of every
  interface. **Operators:** a service that was reached through an interface
  other than `engine_host` is no longer reachable there; set
  `job_bind_address` to expose it deliberately. Job env values may no longer
  contain line breaks.
- The helper validates `runtime_id` against the backend's id shape and puts
  `--` before positional arguments in every Docker call; `inspect` describes
  only containers it launched. The engine artifact mount is resolved and
  checked by path components (no `..`, look-alike prefixes or symlink escapes)
  and passed with `--mount type=bind,…,readonly`; the allowed root is now the
  verified-artifact cache, not the whole state directory. The nonce replay
  window is bounded.
- `engine_host` must be an IP or an RFC 1123 host name; IPv6 endpoints are
  bracketed. Job `log_tail`, `reason` and `message` are bounded in bytes and
  sent as valid UTF-8. A job whose spec fails validation is reported
  `Failed/InvalidSpec` instead of stopping the node from applying its
  snapshot. Metric label values are escaped.
- **Batch jobs and services (DESIGN.md 25).** The agent runs digest-pinned
  containers to completion (`job`) or until stopped (`service`), next to engines.
  Off unless `policy.jobs_enabled` is set. A job is launched at most once, even
  across an agent restart; it holds its GPUs exclusively through the reservation
  ledger; the root helper re-checks image, devices, port, arguments and
  environment and runs the container with no host mounts and no capabilities.
  `contracts/v1` gains an optional `jobs` list on desired and observed state.
  Launches run off the tick (phase `Starting` covers the image pull), a
  restarted agent stops job containers it no longer tracks, and the helper
  survives a failing `docker run` and only touches containers it launched.
- A freshly enrolled managed node now reports its capacity before anything is
  assigned to it, and a host without NVML reports an empty inventory instead of
  failing to start.

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
