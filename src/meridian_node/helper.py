"""Root helper: the only component that performs Docker lifecycle operations
(DESIGN.md 14.1).

The non-root agent sends typed requests; the helper validates the active fencing
epoch, replays via its own root-owned journal, and re-checks every request
against local policy (image digest, devices, port, mounts) before touching the
container backend. It accepts no commands or free-form arguments. The transport
is a Unix socket in production; here the server logic is a pure `handle(dict) ->
dict` so it is testable without a socket, and the socket server is a thin shell
around it.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from typing import Optional, Protocol

from .journal import COMMITTED, Journal


class HelperError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass
class HelperPolicy:
    allowed_image_digests: frozenset[str]
    allowed_devices: frozenset[str]
    port_start: int
    port_stop: int
    allowed_mount_roots: tuple[str, ...]
    # Batch jobs (DESIGN.md 25). All off by default.
    jobs_enabled: bool = False
    job_allow_any_image: bool = False
    allowed_job_image_digests: frozenset[str] = frozenset()
    allowed_job_registries: frozenset[str] = frozenset()
    job_port_start: int = 0
    job_port_stop: int = 0


class ContainerBackend(Protocol):
    def launch(self, spec: dict) -> str: ...
    def inspect(self, runtime_id: str) -> Optional[dict]: ...
    def stop(self, runtime_id: str, force_after_seconds: int) -> None: ...
    def list_all(self) -> list[dict]: ...
    def launch_job(self, spec: dict) -> str: ...
    def logs(self, runtime_id: str, max_bytes: int) -> str: ...
    def remove(self, runtime_id: str) -> None: ...


_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
MAX_JOB_ARGS = 256
MAX_JOB_ENV = 128
MAX_JOB_STRING = 8192
MAX_LOG_BYTES = 65536


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class HelperServer:
    def __init__(self, backend: ContainerBackend, journal: Journal, policy: HelperPolicy):
        self.backend = backend
        self.journal = journal
        self.policy = policy
        self.highest_epoch = 0
        self._seen_nonces: set[str] = set()
        # Connections are served on separate threads. The lock guards the journal and nonce set; it is
        # never held across a backend call, since `docker run` can block for minutes pulling an image.
        self._lock = threading.Lock()

    def refresh_epoch(self, epoch: int) -> None:
        """Agent updates the accepted epoch after session establishment. A lower
        epoch can never be restored (DESIGN.md 14.1)."""
        if epoch < self.highest_epoch:
            raise HelperError("STALE_EPOCH", f"epoch {epoch} < accepted {self.highest_epoch}")
        self.highest_epoch = epoch

    def handle(self, request: dict) -> dict:
        for field in ("operation_id", "agent_session_id", "nonce", "fencing_epoch", "type", "payload"):
            if field not in request:
                raise HelperError("INVALID_REQUEST", f"missing field {field}")
        if request["fencing_epoch"] < self.highest_epoch:
            raise HelperError("STALE_EPOCH", "request carries an outdated fencing epoch")

        payload = request["payload"]
        req_type = request["type"]

        # Reads are safe to repeat: no journal, no nonce consumption.
        if req_type == "inspect":
            return {"ok": True, "result": self.backend.inspect(payload["runtime_id"])}
        if req_type == "list":
            return {"ok": True, "result": self.backend.list_all()}
        if req_type == "logs":
            limit = max(1, min(int(payload.get("max_bytes", MAX_LOG_BYTES)), MAX_LOG_BYTES))
            if self.backend.inspect(payload["runtime_id"]) is None:
                return {"ok": True, "result": ""}
            self._require_owned(payload["runtime_id"])
            return {"ok": True, "result": self.backend.logs(payload["runtime_id"], limit)}

        op_id = request["operation_id"]
        digest = _digest({"type": req_type, "payload": payload})

        # Idempotent replay wins first: a committed op returns its stored result
        # regardless of nonce, so a legitimate retry is always safe.
        with self._lock:
            prior = self.journal.plan(op_id, digest, payload.get("engine_id", ""), request["fencing_epoch"], req_type)
            if prior == COMMITTED:
                row = self.journal.inspect(op_id)
                return {"ok": True, "result": row.result if row else None, "replayed": True}

            # A fresh (uncommitted) mutation must carry an unused nonce.
            nonce = request["nonce"]
            if nonce in self._seen_nonces:
                raise HelperError("REPLAY", "nonce already used")
            self._seen_nonces.add(nonce)
        result = self._execute(req_type, payload)
        with self._lock:
            self.journal.mark(op_id, COMMITTED, result=result)
        return {"ok": True, "result": result}

    def _execute(self, req_type: str, payload: dict) -> str:
        if req_type == "launch":
            self._validate_launch(payload)
            return self.backend.launch(payload)
        if req_type == "launch_job":
            self._validate_job(payload)
            return self.backend.launch_job(payload)
        if req_type == "stop":
            self._require_owned(payload["runtime_id"])
            self.backend.stop(payload["runtime_id"], payload.get("force_after_seconds", 30))
            return str(payload["runtime_id"])
        if req_type == "remove":
            self._require_owned(payload["runtime_id"])
            self.backend.remove(payload["runtime_id"])
            return str(payload["runtime_id"])
        raise HelperError("INVALID_REQUEST", f"unknown request type {req_type}")

    def _require_owned(self, runtime_id: str) -> None:
        """Only containers this helper launched (they carry an ownership marker) may be touched."""
        found = self.backend.inspect(runtime_id)
        if not (found and found.get("ownership_marker")):
            raise HelperError("POLICY_DENIED", f"container {runtime_id} was not launched by this helper")

    def _validate_job(self, payload: dict) -> None:
        """Re-check a job against local policy. The agent already did; the helper
        trusts nothing it is sent (DESIGN.md 14.1, 25)."""
        from .models import IMAGE_REF

        p = self.policy
        if not p.jobs_enabled:
            raise HelperError("POLICY_DENIED", "jobs are disabled by this host's policy")
        image = payload.get("image")
        if not isinstance(image, str) or not IMAGE_REF.match(image):
            raise HelperError("POLICY_DENIED", "job image must be a digest-pinned reference")
        if p.allowed_job_registries and image.split("/", 1)[0] not in p.allowed_job_registries:
            raise HelperError("POLICY_DENIED", f"registry not allow-listed: {image.split('/', 1)[0]}")
        if not p.job_allow_any_image and image.rsplit("@", 1)[1] not in p.allowed_job_image_digests:
            raise HelperError("POLICY_DENIED", f"image not allow-listed: {image.rsplit('@', 1)[1]}")
        for dev in payload.get("device_ids", []):
            if dev not in p.allowed_devices:
                raise HelperError("POLICY_DENIED", f"device not allow-listed: {dev}")
        port = payload.get("port", 0)
        if not isinstance(port, int) or (port and not (p.job_port_start <= port < p.job_port_stop)):
            raise HelperError("POLICY_DENIED", f"port {port} outside the job port range")
        args, env = payload.get("args", []), payload.get("env", {})
        if not isinstance(args, list) or len(args) > MAX_JOB_ARGS or not all(
            isinstance(a, str) and len(a) <= MAX_JOB_STRING and "\x00" not in a for a in args
        ):
            raise HelperError("POLICY_DENIED", "job args must be a bounded list of strings")
        if not isinstance(env, dict) or len(env) > MAX_JOB_ENV:
            raise HelperError("POLICY_DENIED", "job env must be a bounded mapping")
        for k, v in env.items():
            if not isinstance(k, str) or not _ENV_NAME.match(k):
                raise HelperError("POLICY_DENIED", f"invalid environment variable name: {k!r}")
            if not isinstance(v, str) or len(v) > MAX_JOB_STRING or "\x00" in v:
                raise HelperError("POLICY_DENIED", f"invalid value for environment variable {k}")
        # A job may never ask for host access: these are not part of the request shape.
        for forbidden in ("mounts", "volumes", "privileged", "network", "cap_add", "devices", "user", "pid"):
            if forbidden in payload:
                raise HelperError("POLICY_DENIED", f"job field not permitted: {forbidden}")

    def _validate_launch(self, payload: dict) -> None:
        p = self.policy
        if payload.get("image_digest") not in p.allowed_image_digests:
            raise HelperError("POLICY_DENIED", f"image not allow-listed: {payload.get('image_digest')}")
        for dev in payload.get("device_ids", []):
            if dev not in p.allowed_devices:
                raise HelperError("POLICY_DENIED", f"device not allow-listed: {dev}")
        port = payload.get("port", -1)
        if not (p.port_start <= port < p.port_stop):
            raise HelperError("POLICY_DENIED", f"port {port} outside allowed range")
        mount = payload.get("artifact_mount", "")
        if not any(mount.startswith(root) for root in p.allowed_mount_roots):
            raise HelperError("POLICY_DENIED", f"mount outside allowed roots: {mount}")
        if "command" in payload or "args_override" in payload:
            raise HelperError("POLICY_DENIED", "free-form command not permitted")


def unix_socket_send(socket_path: str):
    """Client factory: returns a `send(request_dict) -> response_dict` that speaks
    newline-delimited JSON to the helper's Unix socket. Used by HelperRuntime."""
    import socket

    def send(request: dict) -> dict:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path)
            s.sendall((json.dumps(request) + "\n").encode())
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf += chunk
        return dict(json.loads(buf.decode()))

    return send


def serve_unix_socket(server: "HelperServer", socket_path: str) -> None:  # pragma: no cover - daemon loop
    """Blocking helper daemon: one connection = one request. Root-owned socket,
    mode 0660, agent group only (DESIGN.md 14.1). Runs under systemd."""
    import os
    import socket

    if os.path.exists(socket_path):
        os.unlink(socket_path)
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(socket_path)
    os.chmod(socket_path, 0o660)
    srv.listen(16)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=_serve_connection, args=(server, conn), daemon=True).start()


def _serve_connection(server: "HelperServer", conn) -> None:  # pragma: no cover - daemon loop
    with conn:
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = conn.recv(65536)
            if not chunk:
                break
            buf += chunk
        conn.sendall((json.dumps(respond(server, buf)) + "\n").encode())


def respond(server: "HelperServer", raw: bytes) -> dict:
    """One request's bytes in, one response out. Never raises: a failing backend (for example `docker
    run` on an image that does not exist) must not take the daemon down."""
    try:
        return server.handle(json.loads(raw.decode()))
    except HelperError as e:
        return {"ok": False, "code": e.code, "message": e.message}
    except Exception as e:
        message = type(e).__name__
        stderr = getattr(e, "stderr", None)  # subprocess errors: the command line may carry job env values
        if isinstance(stderr, str) and stderr.strip():
            message += ": " + stderr.strip()[-300:]
        return {"ok": False, "code": "BACKEND_ERROR", "message": message}


class DockerContainerBackend:  # pragma: no cover - requires a Docker host (DESIGN.md 20 host tests)
    """Real backend shelling to the Docker CLI. Ownership is tracked with a
    container label so adoption survives helper restarts. Images are launched by
    digest via a policy-provided repository map (never a free-form ref)."""

    LABEL = "dev.meridian.ownership"

    def __init__(self, image_repos: dict[str, str]):
        # image_digest -> "repo@sha256:..." reference the daemon may pull.
        self.image_repos = image_repos

    def _run(self, args: list[str]) -> str:
        import subprocess

        return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout.strip()

    def launch(self, spec: dict) -> str:
        image_ref = self.image_repos.get(spec["image_digest"])
        if not image_ref:
            raise HelperError("POLICY_DENIED", "no repository mapping for image digest")
        args = [
            "run", "-d", "--restart", "no",
            "--label", f"{self.LABEL}={spec['ownership_marker']}",
            "-p", f"{spec['port']}:{spec['port']}",
            "-v", f"{spec['artifact_mount']}:/model:ro",
        ]
        if spec.get("device_ids"):
            args += ["--gpus", '"device=' + ",".join(spec["device_ids"]) + '"']
        if spec.get("memory_limit_bytes"):
            args += ["--memory", str(spec["memory_limit_bytes"])]
        args += [image_ref, *spec.get("args", [])]
        return self._run(args)

    def inspect(self, runtime_id: str) -> Optional[dict]:
        import json as _json

        try:
            out = self._run(["inspect", runtime_id])
        except Exception:
            return None
        data = _json.loads(out)[0]
        labels = data["Config"].get("Labels") or {}
        return {
            "runtime_id": runtime_id,
            "alive": data["State"]["Running"],
            "image_digest": data["Image"],
            "ownership_marker": labels.get(self.LABEL, ""),
            "device_ids": [],
            "port": 0,
            "exit_code": data["State"].get("ExitCode"),
        }

    def stop(self, runtime_id: str, force_after_seconds: int) -> None:
        self._run(["stop", "-t", str(force_after_seconds), runtime_id])

    def list_all(self) -> list[dict]:
        ids = self._run(["ps", "-q", "--filter", f"label={self.LABEL}"]).split()
        return [s for s in (self.inspect(i) for i in ids) if s]

    def launch_job(self, spec: dict) -> str:
        """Run a tenant container with no host access: no mounts, no added
        capabilities, no privilege escalation, a bounded process count. It gets
        only the GPUs it was assigned and, optionally, one published port."""
        args = [
            "run", "-d", "--restart", "no",
            "--label", f"{self.LABEL}={spec['ownership_marker']}",
            "--label", f"dev.meridian.job={spec['job_key']}",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--pids-limit", "8192",
            "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
        ]
        if spec.get("device_ids"):
            args += ["--gpus", '"device=' + ",".join(spec["device_ids"]) + '"']
        if spec.get("memory_limit_bytes"):
            args += ["--memory", str(spec["memory_limit_bytes"])]
        if spec.get("port"):
            args += ["-p", f"{spec['port']}:{spec['port']}"]
        for k, v in sorted((spec.get("env") or {}).items()):
            args += ["-e", f"{k}={v}"]
        args += [spec["image"], *spec.get("args", [])]
        return self._run(args)

    def logs(self, runtime_id: str, max_bytes: int) -> str:
        import subprocess

        r = subprocess.run(
            ["docker", "logs", "--tail", "200", runtime_id], capture_output=True, text=True, errors="replace",
        )
        return (r.stdout + r.stderr)[-max_bytes:]

    def remove(self, runtime_id: str) -> None:
        self._run(["rm", "-f", runtime_id])


class FakeContainerBackend:
    """In-memory backend for helper protocol tests."""

    def __init__(self):
        self._c: dict[str, dict] = {}
        self._n = 0
        self.job_specs: dict[str, dict] = {}
        self.job_logs: dict[str, str] = {}

    def launch(self, spec: dict) -> str:
        self._n += 1
        rid = f"ctr-{self._n}"
        self._c[rid] = {
            "runtime_id": rid, "alive": True, "image_digest": spec["image_digest"],
            "ownership_marker": spec.get("ownership_marker", ""), "device_ids": spec.get("device_ids", []),
            "port": spec.get("port", 0), "exit_code": None,
        }
        return rid

    def inspect(self, runtime_id: str) -> Optional[dict]:
        return self._c.get(runtime_id)

    def stop(self, runtime_id: str, force_after_seconds: int) -> None:
        if runtime_id in self._c:
            self._c[runtime_id]["alive"] = False
            self._c[runtime_id]["exit_code"] = 0

    def list_all(self) -> list[dict]:
        return list(self._c.values())

    def launch_job(self, spec: dict) -> str:
        self._n += 1
        rid = f"ctr-{self._n}"
        self._c[rid] = {
            "runtime_id": rid, "alive": True, "image_digest": spec["image"].rsplit("@", 1)[1],
            "ownership_marker": spec.get("ownership_marker", ""), "device_ids": spec.get("device_ids", []),
            "port": spec.get("port", 0), "exit_code": None,
        }
        self.job_specs[rid] = spec
        return rid

    def logs(self, runtime_id: str, max_bytes: int) -> str:
        return self.job_logs.get(runtime_id, "")[-max_bytes:]

    def remove(self, runtime_id: str) -> None:
        self._c.pop(runtime_id, None)
