"""meridian-node: host-local agent for GPU inference servers.

Phase 0 scope: identity, durable state, enrollment, heartbeat, and lease
handling against a fake control plane. No engine process control exists yet;
see DESIGN.md section 21.
"""

__version__ = "0.1.0"
PROTOCOL_VERSION = "v1"
AGENT_VERSION = __version__
