# Security model

This summarizes the enforced controls. The normative source is
[DESIGN.md](../DESIGN.md) section 15.

## Trust boundaries

The agent is privileged relative to the host, so it assumes the central API can
be compromised independently. mTLS protects transport identity; it does not make
the control plane trusted. The **local, root-owned policy file** is an
independent constraint: it pins allowed image digests, artifact signing keys,
source hosts, ports, mounts, and per-device resource ceilings, and whether
managed mode is enabled at all. The agent rejects desired state outside this
policy even when the request is correctly authenticated.

`management_enabled: false` is a kill switch that stops lifecycle mutations
without deleting evidence or killing running engines.

## Identity and fencing

- Ed25519 node key, created locally with owner-only permissions; the private key
  never leaves the host.
- Enrollment is approval-gated; a lost key requires operator revocation plus a
  new one-time token. A node cannot silently mint a replacement identity.
- Every session carries a server-issued, monotonic `fencing_epoch`. A stale
  agent session can never mutate current state, and a control-plane restore from
  backup cannot regress fencing (incarnation-ordered epochs, DESIGN 17.5).
- **Transport auth is enforced end to end:** after enrollment the agent presents
  its client certificate on every call and the control plane binds the cert's SAN
  `node_id` to the path (a no-cert call is rejected). Certificates **rotate
  automatically** before expiry (DESIGN 15.6). `require_mtls` is default-off for
  dev and enabled behind the TLS-terminating edge.
- **Air-gapped mode** has no live channel, so a **signed desired-state bundle**
  (a dedicated authority key, distinct from artifact signing keys) is the trust
  anchor that decides what may run; unsigned or tampered bundles are refused
  (DESIGN 6.4).

## Privilege separation

- The agent runs non-root with **no Docker socket access**.
- A small root-owned helper performs the only Docker lifecycle operations, over
  a `0660` Unix socket. Every request is re-validated against local policy and
  the active fencing epoch; free-form commands and arbitrary arguments are
  refused. The helper keeps its own root-owned journal for idempotent replay.

## Artifact trust

- Model artifacts are content-addressed and verified by a signed manifest before
  use. A successful HTTP 200 is never sufficient — the digest and an allow-listed
  signing key must match.
- The downloader restricts sources to allow-listed hosts, confines redirects,
  and rejects path traversal, symlinks, and oversize files.

## What is never logged

Prompts, completions, bearer tokens, private keys, model credentials, and
unrestricted environment values. Logs carry IDs, phases, reasons, and error
classes only. Prometheus labels are bounded (DESIGN 19).

## Managed-host GA gates

The controls that previously blocked managed lifecycle are **in place**:
production mTLS issuance and enforcement, certificate rotation, lease fencing
(restore-safe), signed runtime manifests, local policy enforcement, and central
admission withdrawal (the gateway consumes the serving projection). The full
`node ↔ control ↔ gateway` path is verified end to end with mTLS enforced
(DESIGN 15.6, 20).

Remaining hardening: certificate **revocation** automation / CRL distribution
(the revoke flag and deny path exist; automation does not yet), and Alembic
migrations for the control-plane schema.
