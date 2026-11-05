# Installing meridian-node

`meridian-node` runs as a dedicated non-root OS user. Managed-host mode adds a
separate root-owned helper. Start in observe-only mode; enable management only
after the security gates in [SECURITY.md](SECURITY.md) are met.

## From source

Requires **Python 3.11 or newer**; **3.14 is recommended** (the image and CI run
on it). On hosts whose system Python is older (e.g. Ubuntu 22.04 ships 3.10),
install 3.14 with `uv python install 3.14` or your distribution's packages.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[http,dev]"
meridian-node --config /etc/meridian-node/config.yaml check-config
```

## Observe-only (recommended first)

1. Copy `deploy/config.observe-only.yaml` to `/etc/meridian-node/config.yaml`
   and set `control_plane_url` and `display_name`.
2. Obtain a one-time enrollment token from the control plane and export it:
   ```bash
   export MERIDIAN_NODE_TOKEN=<one-time-token>
   ```
3. Run a single cycle to verify enrollment and heartbeat:
   ```bash
   meridian-node --config /etc/meridian-node/config.yaml run --once
   ```
4. Install the systemd unit from `deploy/systemd/meridian-node.service`,
   `systemctl enable --now meridian-node`.

Enrollment is approval-gated by default: the node stays in a pending claim until
an operator approves it (or the token was minted with `auto_approve`).

## Managed-host

1. Install and start the root helper unit
   (`deploy/systemd/meridian-node-helper.service`). It owns the Docker lifecycle
   over a `0660` Unix socket in `/run/meridian-node/`.
2. Use `deploy/config.managed-host.example.yaml` as the template. The file must
   be **owned by root** and not writable by the agent user. It pins the allowed
   image digests, artifact signing keys, source hosts, ports, and per-device GPU
   budget.
3. `management_enabled: false` is the kill switch — it stops lifecycle mutations
   without deleting state or killing running engines.
4. In production, enable mTLS on the control plane (`require_mtls`, behind the
   TLS-terminating edge). The agent presents its enrolled certificate on every
   call and rotates it before expiry automatically.

## Air-gapped

No control channel. Set `mode: air-gapped` with an `air_gap` block (`node_id`,
`inbox_dir`, `outbox_dir`, and one or more `authority_keys`). Desired state
arrives as a **signed bundle** placed in `inbox/desired/`; observations spool to
`outbox/`. Produce bundles with `meridian-node make-bundle` on a machine that
holds the authority private key, then copy them (plus `inbox/manifests/` and
`inbox/artifacts/`) to the host via removable media. See DESIGN.md §6.4.

## Container

```bash
docker build -f deploy/docker/Dockerfile -t meridian-node .
```

The container runs the non-root agent. The root helper runs on the host, not in
the container, because it manages host Docker.
