# Air-gapped mode — design

**Date:** 2026-08-03
**Status:** approved, implementing
**Design ref:** DESIGN.md §6.4, Phase-4 gate ("Air-gap mode can validate local artifacts without registry access")

## Principle

Air-gapped mode swaps only the *edges* of the agent — the network transport and
the artifact source. The reconciler, supervisor, journal, reservation ledger,
drivers, and state machine run **unchanged**, because both edges already sit
behind interfaces (`Transport`, `ManifestSource`, fetcher). The **signature on a
desired-state bundle is the offline trust anchor** that replaces the mTLS session:
online, desired state is trusted because it arrives over an authenticated channel;
offline, it is trusted because it is signed by a configured authority key.

## Decisions (locked)

- **Full offline control loop** with signed bundles (not artifact-only).
- **Identity from local config**: `node_id` is set in the root-owned config; the
  signed bundle is addressed to it, so the signature still gates what runs.
- **Dedicated authority key**: `air_gap.authority_keys` (Ed25519), distinct from
  the artifact-publisher `trusted_signing_keys`, so an artifact publisher cannot
  forge desired state.

## Components (each isolated, one purpose)

1. **`airgap.py` — bundle trust.**
   - `sign_desired_bundle(snapshot, generation, stop_ids, key_id, private_key) -> dict`
   - `verify_desired_bundle(raw, authority_keys) -> VerifiedBundle` (generation,
     snapshot dict, stop_authorized ids). Ed25519 over the canonical JSON of
     `{generation, snapshot, stop_authorized_engine_ids}`. Tampered payload or
     unknown/mismatched key → `BundleError`. This is the security boundary.

2. **`offline_transport.py` — `OfflineTransport`** implements the existing
   `Transport` protocol, so nothing downstream changes:
   - `enroll` → returns the configured `node_id` (no network, no cert).
   - `establish_session` → mints a **local monotonic fencing epoch** (persisted
     counter file in `state_dir`) so the root helper has its epoch. No live-
     takeover concern with a single offline agent.
   - `heartbeat` → reads the latest inbox bundle's generation; returns a synthetic
     lease (`now + lease_ttl`) plus `stop_authorized_engine_ids` from the bundle.
   - `fetch_desired_state` → reads + verifies the highest-generation signed bundle
     in the **inbox** dir; returns the snapshot dict.
   - `post_observation` → writes JSON to the **outbox** spool
     (`observation-<gen>-<seq>.json`) for operator collection.
   - `rotate_certificate` → no-op (no live TLS offline).

3. **Local artifact source** (`transfer.py`): `LocalManifestSource` +
   `LocalFetcher` read manifests/files from the inbox bundle dir. The existing
   `verify_manifest` digest/signature path is untouched — that is the Phase-4 gate.

4. **`config.py` — `AirGapConfig`**: `node_id`, `inbox_dir`, `outbox_dir`,
   `authority_keys` (key_id → b64url pubkey). Fail closed: mode `air-gapped`
   requires `node_id` and at least one authority key.

5. **`wiring.py`** — branch on `mode == "air-gapped"`: build `OfflineTransport` +
   local manifest/fetcher instead of the HTTP ones. `managed_allowed()` and
   `attach_reconciler` broaden to `mode in ("managed-host", "air-gapped")` so an
   air-gapped node launches engines from local bundles.

6. **CLI `make-bundle`** — operator tooling: sign a desired-state snapshot into a
   bundle file with an authority private key. Minimal; reuses `sign_desired_bundle`.

## Data flow

Operator signs a bundle → copies it (+ manifests + artifact files) to the node's
inbox via removable media → agent each cycle: `heartbeat` (reads gen) →
`fetch_desired_state` (verifies bundle) → reconcile (launches/verifies engines
from local artifacts) → `post_observation` (spools to outbox). Operator later
collects the outbox for offline import to the control plane.

## Error handling

- Missing/unverifiable bundle → hold last-known desired state, log, never
  fabricate desired state (fail-safe; mirrors the gateway's keep-last-good).
- Bad signature → reject that bundle, never apply.
- Generation regression → already refused by the reconciler / state store.

## Testing

- Unit: `verify_desired_bundle` valid / tampered / wrong-key; `OfflineTransport`
  read-bundle, spool-observation, monotonic-epoch.
- **Integration (the Phase-4 gate):** a full air-gapped managed loop — sign a
  bundle with an assignment + local manifest + artifact, `agent.tick()` drives
  Warming→Ready, observation lands in the outbox. The offline mirror of
  `tests/test_live_server.py`.

## Scope guard (YAGNI)

No bundle encryption (integrity + authenticity via signature is the requirement,
not secrecy). No automatic removable-media mounting (operator drops files in the
inbox). No offline cert (inert without a live TLS session).
