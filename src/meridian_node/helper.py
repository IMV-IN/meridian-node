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
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol

from .journal import COMMITTED, Journal
from .models import DEVICE_ID, IMAGE_REF, JOB_ID, MAX_IMAGE_REF, job_strings_error


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
    # Job container limits (DESIGN.md 25.3). The helper applies these itself;
    # the agent cannot send them.
    job_bind_address: str = "127.0.0.1"  # IP literal published job ports bind to
    job_memory_default_bytes: int = 32 * 1024**3  # cap for a job that asks for none
    job_memory_max_bytes: int = 256 * 1024**3  # largest cap a job may ask for
    job_cpus: float = 0.0  # 0 = no CPU limit


class ContainerBackend(Protocol):
    # Grammar of the runtime ids this backend hands out. Every id the agent
    # sends back is checked against it before it reaches the backend.
    RUNTIME_ID: re.Pattern[str]

    def launch(self, spec: dict) -> str: ...
    def inspect(self, runtime_id: str) -> Optional[dict]: ...
    def stop(self, runtime_id: str, force_after_seconds: int) -> None: ...
    def list_all(self) -> list[dict]: ...
    def launch_job(self, spec: dict) -> str: ...
    def logs(self, runtime_id: str, max_bytes: int) -> str: ...
    def remove(self, runtime_id: str) -> None: ...


MAX_LOG_BYTES = 65536
MAX_NONCES = 65536  # replay window; older nonces are forgotten first
# Every field a launch_job request may carry. Anything else is refused, so a
# new host-access knob can never be smuggled in under a name the helper ignores.
JOB_FIELDS = frozenset({
    "engine_id", "job_key", "image", "args", "env", "device_ids", "port", "memory_limit_bytes", "ownership_marker",
})
# "<node_id>:<job_key>:<operation_id>", written into a container label.
OWNERSHIP_MARKER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,511}")
# A bind-mount source: no comma, quote, colon or '=', so it stays one field of `--mount`.
MOUNT_PATH = re.compile(r"/[A-Za-z0-9._/+-]{1,4095}")


def gpu_env(device_ids: list[str]) -> dict[str, str]:
    """GPU visibility the helper sets for every job container, after the job's
    own env, so neither the job nor its image's ENV can widen it (H4). With no
    devices, `void` denies GPUs even where the host's default runtime is
    nvidia and honours this variable."""
    if device_ids:
        return {
            "NVIDIA_VISIBLE_DEVICES": ",".join(device_ids),
            "NVIDIA_DRIVER_CAPABILITIES": "compute,utility",
            "CUDA_VISIBLE_DEVICES": ",".join(str(i) for i in range(len(device_ids))),
        }
    return {
        "NVIDIA_VISIBLE_DEVICES": "void", "NVIDIA_DRIVER_CAPABILITIES": "compute,utility", "CUDA_VISIBLE_DEVICES": "",
    }


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class HelperServer:
    def __init__(self, backend: ContainerBackend, journal: Journal, policy: HelperPolicy):
        self.backend = backend
        self.journal = journal
        self.policy = policy
        self.highest_epoch = 0
        self._seen_nonces: dict[str, None] = {}  # insertion-ordered, bounded by MAX_NONCES
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

        if not isinstance(payload, dict):
            raise HelperError("INVALID_REQUEST", "payload must be an object")

        # Reads are safe to repeat: no journal, no nonce consumption.
        if req_type == "inspect":
            found = self.backend.inspect(self._runtime_id(payload))
            # Only containers this helper launched are described to the agent.
            return {"ok": True, "result": found if found and found.get("ownership_marker") else None}
        if req_type == "list":
            return {"ok": True, "result": self.backend.list_all()}
        if req_type == "logs":
            runtime_id = self._runtime_id(payload)
            max_bytes = payload.get("max_bytes", MAX_LOG_BYTES)
            if type(max_bytes) is not int:
                raise HelperError("INVALID_REQUEST", "max_bytes must be an integer")
            limit = max(1, min(max_bytes, MAX_LOG_BYTES))
            if self.backend.inspect(runtime_id) is None:
                return {"ok": True, "result": ""}
            self._require_owned(runtime_id)
            return {"ok": True, "result": self.backend.logs(runtime_id, limit)}

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
            self._seen_nonces[nonce] = None
            if len(self._seen_nonces) > MAX_NONCES:
                del self._seen_nonces[next(iter(self._seen_nonces))]
        result = self._execute(req_type, payload)
        with self._lock:
            self.journal.mark(op_id, COMMITTED, result=result)
        return {"ok": True, "result": result}

    def _execute(self, req_type: str, payload: dict) -> str:
        if req_type == "launch":
            mount = self._validate_launch(payload)
            return self.backend.launch({**payload, "artifact_mount": mount})
        if req_type == "launch_job":
            return self.backend.launch_job(self._validate_job(payload))
        if req_type == "stop":
            runtime_id = self._runtime_id(payload)
            grace = payload.get("force_after_seconds", 30)
            if type(grace) is not int or not 0 <= grace <= 3600:
                raise HelperError("INVALID_REQUEST", "force_after_seconds must be an integer from 0 to 3600")
            self._require_owned(runtime_id)
            self.backend.stop(runtime_id, grace)
            return runtime_id
        if req_type == "remove":
            runtime_id = self._runtime_id(payload)
            self._require_owned(runtime_id)
            self.backend.remove(runtime_id)
            return runtime_id
        raise HelperError("INVALID_REQUEST", "unknown request type")

    def _runtime_id(self, payload: dict) -> str:
        """The runtime id must have the shape this backend hands out, so it can
        never be read as a docker option or name some other kind of object."""
        runtime_id = payload.get("runtime_id")
        if not isinstance(runtime_id, str) or not self.backend.RUNTIME_ID.fullmatch(runtime_id):
            raise HelperError("POLICY_DENIED", "invalid runtime id")
        return runtime_id

    def _require_owned(self, runtime_id: str) -> None:
        """Only containers this helper launched (they carry an ownership marker) may be touched."""
        found = self.backend.inspect(runtime_id)
        if not (found and found.get("ownership_marker")):
            raise HelperError("POLICY_DENIED", f"container {runtime_id} was not launched by this helper")

    def _validate_job(self, payload: dict) -> dict:
        """Re-check a job against local policy and return the spec the backend
        runs. The agent already checked; the helper trusts nothing it is sent
        (DESIGN.md 14.1, 25). Platform fields (memory cap, CPUs, bind address)
        come from the helper's own policy, never from the request."""
        p = self.policy
        if not p.jobs_enabled:
            raise HelperError("POLICY_DENIED", "jobs are disabled by this host's policy")
        # A job may never ask for host access: only known fields are accepted.
        for name in payload:
            if name not in JOB_FIELDS:
                raise HelperError("POLICY_DENIED", "job field not permitted: " + repr(name)[:64])
        image = payload.get("image")
        if not isinstance(image, str) or len(image) > MAX_IMAGE_REF or not IMAGE_REF.fullmatch(image):
            raise HelperError("POLICY_DENIED", "job image must be a digest-pinned reference")
        if p.allowed_job_registries and image.split("/", 1)[0] not in p.allowed_job_registries:
            raise HelperError("POLICY_DENIED", f"registry not allow-listed: {image.split('/', 1)[0]}")
        if not p.job_allow_any_image and image.rsplit("@", 1)[1] not in p.allowed_job_image_digests:
            raise HelperError("POLICY_DENIED", f"image not allow-listed: {image.rsplit('@', 1)[1]}")
        job_key, marker = payload.get("job_key"), payload.get("ownership_marker")
        if not isinstance(job_key, str) or not JOB_ID.fullmatch(job_key):
            raise HelperError("POLICY_DENIED", "invalid job key")
        if not isinstance(marker, str) or not OWNERSHIP_MARKER.fullmatch(marker):
            raise HelperError("POLICY_DENIED", "invalid ownership marker")
        devices = payload.get("device_ids", [])
        if not isinstance(devices, list) or len(set(devices)) != len(devices):
            raise HelperError("POLICY_DENIED", "device_ids must be a list of distinct devices")
        for dev in devices:
            if not isinstance(dev, str) or not DEVICE_ID.fullmatch(dev) or dev not in p.allowed_devices:
                raise HelperError("POLICY_DENIED", "device not allow-listed: " + repr(dev)[:140])
        port = payload.get("port", 0)
        if type(port) is not int or (port and not (p.job_port_start <= port < p.job_port_stop)):
            raise HelperError("POLICY_DENIED", f"port {port!r:.16} outside the job port range")
        memory = payload.get("memory_limit_bytes", 0)
        if type(memory) is not int or memory < 0:
            raise HelperError("POLICY_DENIED", "memory_limit_bytes must be a non-negative integer")
        if memory > p.job_memory_max_bytes:
            raise HelperError(
                "POLICY_DENIED", f"memory limit above this host's maximum of {p.job_memory_max_bytes} bytes"
            )
        args, env = payload.get("args", []), payload.get("env", {})
        error = job_strings_error(args, env)
        if error:
            raise HelperError("POLICY_DENIED", error)
        return {
            "job_key": job_key, "ownership_marker": marker, "image": image, "args": list(args), "env": dict(env),
            "device_ids": list(devices), "port": port,
            "memory_limit_bytes": memory or p.job_memory_default_bytes,
            "cpus": p.job_cpus, "bind_address": p.job_bind_address,
        }

    def _validate_launch(self, payload: dict) -> str:
        """Check an engine launch; returns the resolved artifact mount source."""
        p = self.policy
        if payload.get("image_digest") not in p.allowed_image_digests:
            raise HelperError("POLICY_DENIED", f"image not allow-listed: {payload.get('image_digest')}")
        for dev in payload.get("device_ids", []):
            if not isinstance(dev, str) or not DEVICE_ID.fullmatch(dev) or dev not in p.allowed_devices:
                raise HelperError("POLICY_DENIED", f"device not allow-listed: {dev}")
        port = payload.get("port", -1)
        if type(port) is not int or not (p.port_start <= port < p.port_stop):
            raise HelperError("POLICY_DENIED", f"port {port} outside allowed range")
        if "command" in payload or "args_override" in payload:
            raise HelperError("POLICY_DENIED", "free-form command not permitted")
        return self._mount_source(payload.get("artifact_mount"))

    def _mount_source(self, mount: object) -> str:
        """Resolve the artifact mount and require it strictly inside an allowed
        root by path components: `..`, look-alike prefixes (`/root-evil`) and
        symlinks that lead outside are refused. The resolved path is what docker
        mounts, so docker never follows a link the helper did not check."""
        if not isinstance(mount, str) or not mount.startswith("/") or "\x00" in mount or ".." in mount.split("/"):
            raise HelperError("POLICY_DENIED", "artifact mount must be an absolute path without '..'")
        real = Path(os.path.realpath(mount))
        if not MOUNT_PATH.fullmatch(str(real)):
            raise HelperError("POLICY_DENIED", "artifact mount path has characters outside [A-Za-z0-9._/+-]")
        for root in self.policy.allowed_mount_roots:
            real_root = Path(os.path.realpath(root))
            if real != real_root and real.is_relative_to(real_root):
                return str(real)
        raise HelperError("POLICY_DENIED", "artifact mount outside allowed roots")


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
    digest via a policy-provided repository map (never a free-form ref).

    Every value is its own argv element after the option it belongs to, and
    `--` ends the options before any positional argument."""

    LABEL = "dev.meridian.ownership"
    RUNTIME_ID = re.compile(r"[0-9a-f]{12,64}")  # `docker run -d` and `docker ps -q` ids
    # One user-defined bridge for every job container, with inter-container
    # traffic disabled: a job cannot reach another job's container directly,
    # only ports published on the bind address (DESIGN.md 25.3). The fixed
    # interface name lets an operator add egress rules for it.
    JOB_NETWORK = "meridian-jobs"
    JOB_NETWORK_OPTS = ("com.docker.network.bridge.enable_icc=false", "com.docker.network.bridge.name=meridian-jobs")

    def __init__(self, image_repos: dict[str, str], env_dir: Optional[str] = None):
        # image_digest -> "repo@sha256:..." reference the daemon may pull.
        self.image_repos = image_repos
        # Where job env files live for the moment `docker run` reads them. None = the system temp dir.
        self.env_dir = env_dir

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
            # The source was resolved and checked by HelperServer._mount_source.
            "--mount", f"type=bind,source={spec['artifact_mount']},target=/model,readonly",
        ]
        if spec.get("device_ids"):
            args += ["--gpus", '"device=' + ",".join(spec["device_ids"]) + '"']
        if spec.get("memory_limit_bytes"):
            args += ["--memory", str(spec["memory_limit_bytes"])]
        args += ["--", image_ref, *spec.get("args", [])]
        return self._run(args)

    def inspect(self, runtime_id: str) -> Optional[dict]:
        import json as _json

        try:
            out = self._run(["inspect", "--type", "container", "--", runtime_id])
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
        self._run(["stop", "-t", str(int(force_after_seconds)), "--", runtime_id])

    def list_all(self) -> list[dict]:
        ids = self._run(["ps", "-q", "--filter", f"label={self.LABEL}"]).split()
        return [s for s in (self.inspect(i) for i in ids if self.RUNTIME_ID.fullmatch(i)) if s]

    def _job_network(self) -> str:
        """Create the job network on first use and refuse one that lets
        containers talk to each other (for example one created by hand)."""
        import subprocess

        icc = ["network", "inspect", "--format", '{{index .Options "com.docker.network.bridge.enable_icc"}}', "--"]
        try:
            found = self._run([*icc, self.JOB_NETWORK])
        except subprocess.CalledProcessError:
            create = ["network", "create", "--driver", "bridge", "--label", f"{self.LABEL}=network"]
            for opt in self.JOB_NETWORK_OPTS:
                create += ["--opt", opt]
            try:
                self._run([*create, "--", self.JOB_NETWORK])
            except subprocess.CalledProcessError:
                pass  # created by a concurrent launch; checked below
            found = self._run([*icc, self.JOB_NETWORK])
        if found != "false":
            raise HelperError(
                "POLICY_DENIED", f"network {self.JOB_NETWORK} exists with inter-container traffic enabled"
            )
        return self.JOB_NETWORK

    def launch_job(self, spec: dict) -> str:
        """Run a tenant container with no host access: no mounts, no added
        capabilities, no privilege escalation, bounded processes and memory, its
        own network with no path to other jobs' containers, only its assigned
        GPUs and, optionally, one port published on the policy's bind address.

        Env goes through a 0600 file, never the command line, so other local
        users cannot read it from the process list. `spec` is the output of
        HelperServer._validate_job."""
        import tempfile

        args = [
            "run", "-d", "--restart", "no",
            "--label", f"{self.LABEL}={spec['ownership_marker']}",
            "--label", f"dev.meridian.job={spec['job_key']}",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--pids-limit", "8192",
            "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
            "--network", self._job_network(),
            # Swap equal to memory: the cap is not stretched by swapping. A job is
            # the first thing the kernel kills if the host still runs out.
            "--memory", str(spec["memory_limit_bytes"]), "--memory-swap", str(spec["memory_limit_bytes"]),
            "--oom-score-adj", "500",
        ]
        if spec["cpus"]:
            args += ["--cpus", str(spec["cpus"])]
        if spec["device_ids"]:
            args += ["--gpus", '"device=' + ",".join(spec["device_ids"]) + '"']
        if spec["port"]:
            host = spec["bind_address"]
            host = f"[{host}]" if ":" in host else host
            args += ["-p", f"{host}:{spec['port']}:{spec['port']}"]
        # Platform values last and merged over the job's own: they always win.
        env = {**spec["env"], **gpu_env(spec["device_ids"])}
        fd, env_file = tempfile.mkstemp(prefix="meridian-job-env-", dir=self.env_dir)  # mode 0600
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.writelines(f"{k}={v}\n" for k, v in env.items())
            args += ["--env-file", env_file, "--", spec["image"], *spec["args"]]
            return self._run(args)
        finally:
            os.unlink(env_file)  # `docker run -d` has read it and started the container

    def logs(self, runtime_id: str, max_bytes: int) -> str:
        import subprocess

        r = subprocess.run(
            ["docker", "logs", "--tail", "200", "--", runtime_id], capture_output=True, text=True, errors="replace",
        )
        return (r.stdout + r.stderr)[-max_bytes:]

    def remove(self, runtime_id: str) -> None:
        self._run(["rm", "-f", "--", runtime_id])


class FakeContainerBackend:
    """In-memory backend for helper protocol tests."""

    RUNTIME_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")  # Docker's container-name grammar

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
