# Meridian Node Agent

`meridian-node` is the host-local agent for GPU inference servers. It connects
to central Meridian, reports GPU/model/engine state, reconciles approved desired
state, manages verified artifacts, and supervises inference engines on a GPU
host.

The design is currently an RFC. Read [`DESIGN.md`](DESIGN.md) before adding
implementation code. It defines the repository boundary, first-release
deployment profile, control-plane protocol, identity and fencing model, artifact
trust chain, engine lifecycle, drain semantics, security policy, testing strategy,
and delivery gates.

The initial managed profile is deliberately narrow: Linux, systemd, Docker
Engine, NVIDIA GPUs, pinned vLLM OCI images, PostgreSQL-backed central control
state, and private gateway-to-engine networking. Observe-only mode can support
additional environments without granting them lifecycle-control privileges.
