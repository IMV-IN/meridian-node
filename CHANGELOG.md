# Changelog

## 0.1.0 (unreleased)

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

### Not yet included
- Production central Meridian control plane wiring (compatibility harness in
  `tests/test_compat.py`, gated on `MERIDIAN_COMPAT_URL`).
- Kubernetes-integrated and air-gapped modes (Phase 4).
- Placement and capacity scheduling (Phase 5).
- Certificate rotation/revocation automation and full host tests on GPU
  hardware.

Tests: 69 passing (+ 1 skipped external compat); `ruff` and `mypy` clean.
