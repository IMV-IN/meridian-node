"""Job isolation and the helper's trust boundary (security audit H4, M8 and the
node-side Low/Info items; second review F1-F3). The Docker backend's argv is
captured instead of run, so each test asserts the exact command the root
helper would execute."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from meridian_node import helper as helper_mod
from meridian_node import jobs as jobs_mod
from meridian_node.config import Config, Policy
from meridian_node.helper import DockerContainerBackend, FakeContainerBackend, HelperError, HelperPolicy, HelperServer
from meridian_node.jobs import LOG_TAIL_BYTES, JobPolicy, JobSupervisor
from meridian_node.journal import Journal
from meridian_node.models import DesiredSnapshot, JobAssignment
from meridian_node.reservation import ReservationLedger
from meridian_node.runtime import FakeRuntime

IMAGE = "docker.io/pytorch/pytorch@sha256:" + "b" * 64
RID = "a" * 64
GIB = 1024**3


class CapturingDocker(DockerContainerBackend):
    """The real Docker backend with `docker` replaced by a recorder."""

    def __init__(self, tmp_path: Path, network: str | None = "false"):
        super().__init__({"sha256:" + "e" * 64: "docker.io/vllm/vllm@sha256:" + "e" * 64}, env_dir=str(tmp_path))
        self.network = network  # the network's enable_icc option, None = no network yet
        self.calls: list[list[str]] = []
        self.env_file: tuple[str, str, int] | None = None  # path, content, mode while docker reads it

    def _run(self, args: list[str]) -> str:
        self.calls.append(args)
        if args[:2] == ["network", "inspect"]:
            if self.network is None:
                raise subprocess.CalledProcessError(1, args)
            return self.network
        if args[:2] == ["network", "create"]:
            self.network = "false"
            return "net"
        if "--env-file" in args:
            path = args[args.index("--env-file") + 1]
            self.env_file = (path, Path(path).read_text(), stat.S_IMODE(os.stat(path).st_mode))
        if args[:1] == ["inspect"]:
            raise subprocess.CalledProcessError(1, args)
        return RID


def _policy(tmp_path, **over) -> HelperPolicy:
    base: dict = dict(
        allowed_image_digests=frozenset({"sha256:" + "e" * 64}), allowed_devices=frozenset({"GPU-0", "GPU-1"}),
        port_start=8000, port_stop=8100, allowed_mount_roots=(str(tmp_path / "artifacts"),),
        jobs_enabled=True, job_allow_any_image=True, job_port_start=9000, job_port_stop=9010,
        job_bind_address="10.0.0.5",
    )
    return HelperPolicy(**{**base, **over})


def _server(tmp_path, backend=None, **policy):
    tmp_path.mkdir(parents=True, exist_ok=True)
    backend = backend if backend is not None else CapturingDocker(tmp_path)
    return HelperServer(backend, Journal(tmp_path / "helper.db"), _policy(tmp_path, **policy)), backend


def _job(op="op1", **payload) -> dict:
    base = {"engine_id": "job_a.0", "job_key": "job_a.0", "image": IMAGE, "args": ["python", "t.py"], "env": {},
            "device_ids": [], "port": 0, "memory_limit_bytes": 0, "ownership_marker": "n:job_a.0:job:job_a:0"}
    return {"operation_id": op, "agent_session_id": "s", "nonce": op + "-n", "fencing_epoch": 1,
            "type": "launch_job", "payload": {**base, **payload}}


def _run_argv(backend: CapturingDocker) -> list[str]:
    return next(c for c in backend.calls if c[:1] == ["run"])


HARDENING = [
    "run", "-d", "--restart", "no",
    "--label", "dev.meridian.ownership=n:job_a.0:job:job_a:0",
    "--label", "dev.meridian.job=job_a.0",
    "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
    "--pids-limit", "8192",
    "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
    "--network", "meridian-jobs",
]


# --- H4: GPU visibility -----------------------------------------------------
def test_zero_gpu_job_gets_gpus_denied_and_no_gpus_flag(tmp_path):
    server, docker = _server(tmp_path)
    server.handle(_job(env={"HF_HOME": "/cache", "SECRET": "s3cr3t --privileged"}, port=9001))
    argv = _run_argv(docker)
    assert docker.env_file is not None
    env_path, env_text, mode = docker.env_file
    assert argv == [
        *HARDENING,
        "--memory", str(32 * GIB), "--memory-swap", str(32 * GIB), "--oom-score-adj", "500",
        "-p", "10.0.0.5:9001:9001",
        "--env-file", env_path, "--", IMAGE, "python", "t.py",
    ]
    # Platform values come last, after the job's own.
    assert env_text.splitlines() == [
        "HF_HOME=/cache", "SECRET=s3cr3t --privileged",
        "NVIDIA_VISIBLE_DEVICES=void", "NVIDIA_DRIVER_CAPABILITIES=compute,utility", "CUDA_VISIBLE_DEVICES=",
    ]


def test_assigned_gpus_are_exactly_the_reserved_ones(tmp_path):
    server, docker = _server(tmp_path)
    server.handle(_job(device_ids=["GPU-0", "GPU-1"]))
    argv = _run_argv(docker)
    assert docker.env_file is not None
    assert argv == [
        *HARDENING,
        "--memory", str(32 * GIB), "--memory-swap", str(32 * GIB), "--oom-score-adj", "500",
        "--gpus", '"device=GPU-0,GPU-1"',
        "--env-file", docker.env_file[0], "--", IMAGE, "python", "t.py",
    ]
    assert docker.env_file[1].splitlines() == [
        "NVIDIA_VISIBLE_DEVICES=GPU-0,GPU-1", "NVIDIA_DRIVER_CAPABILITIES=compute,utility", "CUDA_VISIBLE_DEVICES=0,1",
    ]


@pytest.mark.parametrize("name", [
    "NVIDIA_VISIBLE_DEVICES", "NVIDIA_DRIVER_CAPABILITIES", "CUDA_VISIBLE_DEVICES",
    "NVIDIA_IMEX_CHANNELS", "NVIDIA_MIG_CONFIG_DEVICES", "CUDA_MPS_PIPE_DIRECTORY",
])
def test_reserved_gpu_env_is_rejected_by_the_model_and_by_the_helper(tmp_path, name):
    with pytest.raises(ValidationError, match="reserved"):
        JobAssignment(job_id="job_a", image=IMAGE, env={name: "all"})
    server, docker = _server(tmp_path)
    with pytest.raises(HelperError) as e:
        server.handle(_job(env={name: "all"}))  # an agent that skipped its own check
    assert e.value.code == "POLICY_DENIED" and "reserved" in e.value.message
    assert not any(c[:1] == ["run"] for c in docker.calls)


def test_ordinary_and_control_plane_env_is_accepted():
    # MERIDIAN_* is set by the control plane inside `env` (it reserves them from users at submit).
    j = JobAssignment(job_id="job_a", image=IMAGE, env={"NVIDIA_TF32_OVERRIDE": "0", "MERIDIAN_JOB_RANK": "0"})
    assert j.env["MERIDIAN_JOB_RANK"] == "0"


@pytest.mark.parametrize("field,value", [
    ("env", {"A\n": "x"}),  # `$` in a regex would accept the trailing newline
    ("env", {"-e": "x"}),
    ("env", {"A": "line1\nNVIDIA_VISIBLE_DEVICES=all"}),  # would add a line to the env file
    ("env", {"A": "x\r"}),
    ("env", {"A": "nul\x00"}),
    ("env", {"A": "é" * 4097}),  # 8194 bytes
    ("env", {f"V{i}": "x" for i in range(129)}),
    ("args", ["x"] * 257),
    ("args", ["é" * 4097]),
    ("args", ["x" * 8000] * 33),  # each fits, the total does not
    ("image", IMAGE + "\n"),
    ("image", "-v/:/h" + IMAGE),
    ("job_id", "-rf"),
    ("job_id", "a:b"),
    ("device_ids", ["GPU-0,GPU-1"]),
])
def test_job_fields_are_bounded_and_allow_listed(tmp_path, field, value):
    with pytest.raises(ValidationError):
        JobAssignment(**{"job_id": "job_a", "image": IMAGE, field: value})
    if field == "job_id":
        field, value = "job_key", value
    server, docker = _server(tmp_path)
    with pytest.raises(HelperError) as e:
        server.handle(_job(**{field: value}))
    assert e.value.code == "POLICY_DENIED"
    assert not any(c[:1] == ["run"] for c in docker.calls)


@pytest.mark.parametrize("field", ["cpus", "bind_address", "network", "runtime", "gpus", "user"])
def test_helper_accepts_only_known_job_fields(tmp_path, field):
    server, _ = _server(tmp_path)
    with pytest.raises(HelperError, match="not permitted"):
        server.handle(_job(**{field: "x"}))


# --- M8: container isolation and limits --------------------------------------
def test_memory_cap_defaults_from_policy_and_is_bounded(tmp_path):
    server, docker = _server(tmp_path, job_memory_default_bytes=4 * GIB, job_memory_max_bytes=8 * GIB, job_cpus=2.5)
    server.handle(_job(op="a", memory_limit_bytes=0))
    server.handle(_job(op="b", memory_limit_bytes=6 * GIB, job_key="job_b.0", ownership_marker="n:job_b.0:j"))
    runs = [c for c in docker.calls if c[:1] == ["run"]]
    for argv, cap in zip(runs, (4 * GIB, 6 * GIB)):
        i = argv.index("--memory")
        assert argv[i:i + 8] == ["--memory", str(cap), "--memory-swap", str(cap), "--oom-score-adj", "500",
                                 "--cpus", "2.5"]
    with pytest.raises(HelperError, match="maximum"):
        server.handle(_job(op="c", memory_limit_bytes=9 * GIB))
    big = JobAssignment(job_id="job_a", image=IMAGE, memory_limit_bytes=9 * GIB)
    assert "maximum" in (JobPolicy(enabled=True, allow_any_image=True, max_memory_bytes=8 * GIB).check(big) or "")


def test_published_port_binds_to_the_configured_address(tmp_path):
    for bind, expected in (("10.0.0.5", "10.0.0.5:9003:9003"), ("::1", "[::1]:9003:9003")):
        server, docker = _server(tmp_path / bind.replace(":", "_"), job_bind_address=bind)
        server.handle(_job(port=9003))
        argv = _run_argv(docker)
        assert argv[argv.index("-p") + 1] == expected
        assert not any(a.startswith(("0.0.0.0", "9003:")) for a in argv)


def test_job_network_is_created_with_inter_container_traffic_disabled(tmp_path):
    server, docker = _server(tmp_path, backend=CapturingDocker(tmp_path, network=None))
    server.handle(_job())
    assert ["network", "create", "--driver", "bridge", "--label", "dev.meridian.ownership=network",
            "--opt", "com.docker.network.bridge.enable_icc=false",
            "--opt", "com.docker.network.bridge.name=meridian-jobs", "--", "meridian-jobs"] in docker.calls
    assert _run_argv(docker)[HARDENING.index("--network"):][:2] == ["--network", "meridian-jobs"]


def test_a_job_network_that_allows_inter_container_traffic_is_refused(tmp_path):
    server, docker = _server(tmp_path, backend=CapturingDocker(tmp_path, network="true"))
    with pytest.raises(HelperError, match="inter-container"):
        server.handle(_job())
    assert not any(c[:1] == ["run"] for c in docker.calls)


# --- F2: env never on the command line ---------------------------------------
def test_env_goes_through_a_private_file_that_is_removed(tmp_path):
    server, docker = _server(tmp_path)
    server.handle(_job(env={"HF_TOKEN": "hf_secret_value"}))
    argv = _run_argv(docker)
    assert docker.env_file is not None
    path, text, mode = docker.env_file
    assert mode == 0o600 and "HF_TOKEN=hf_secret_value\n" in text
    assert "-e" not in argv and not any("hf_secret_value" in a for a in argv)
    assert not Path(path).exists()


# --- F3 / I1: runtime ids ---------------------------------------------------
@pytest.mark.parametrize("kind", ["inspect", "logs", "stop", "remove"])
@pytest.mark.parametrize("rid", ["--help", "-f", "abc", "meridian-jobs", "A" * 64, "a" * 65, 7])
def test_runtime_ids_must_have_the_backend_shape(tmp_path, kind, rid):
    server, docker = _server(tmp_path)
    with pytest.raises(HelperError) as e:
        server.handle({"operation_id": "o", "agent_session_id": "s", "nonce": "n", "fencing_epoch": 1,
                       "type": kind, "payload": {"runtime_id": rid}})
    assert e.value.code == "POLICY_DENIED" and docker.calls == []


def test_docker_commands_end_options_before_the_runtime_id(tmp_path):
    docker = CapturingDocker(tmp_path)
    docker.stop(RID, 10)
    docker.remove(RID)
    docker.inspect(RID)
    assert docker.calls == [
        ["stop", "-t", "10", "--", RID], ["rm", "-f", "--", RID], ["inspect", "--type", "container", "--", RID],
    ]


def test_inspect_does_not_describe_containers_the_helper_did_not_launch(tmp_path):
    server, backend = _server(tmp_path, backend=FakeContainerBackend())
    backend._c["foreign"] = {"runtime_id": "foreign", "alive": True, "image_digest": "x", "ownership_marker": "",
                             "device_ids": [], "port": 0, "exit_code": None}
    resp = server.handle({"operation_id": "o", "agent_session_id": "s", "nonce": "n", "fencing_epoch": 1,
                          "type": "inspect", "payload": {"runtime_id": "foreign"}})
    assert resp["result"] is None


# --- F1: engine artifact mount ----------------------------------------------
def _engine(mount: str) -> dict:
    return {"operation_id": "e" + mount, "agent_session_id": "s", "nonce": "n" + mount, "fencing_epoch": 1,
            "type": "launch", "payload": {
                "engine_id": "e1", "image_digest": "sha256:" + "e" * 64, "device_ids": ["GPU-0"], "port": 8000,
                "artifact_mount": mount, "ownership_marker": "n:e1:op1"}}


def test_artifact_mount_is_resolved_and_compared_by_path_component(tmp_path):
    root = tmp_path / "artifacts"
    (root / "abc").mkdir(parents=True)
    (tmp_path / "artifacts-evil").mkdir()
    (tmp_path / "secret").mkdir()
    (root / "link").symlink_to(tmp_path / "secret")
    for bad in (str(root / ".." / "secret"), str(tmp_path / "artifacts-evil"), str(root / "link"), str(root),
                "relative/path", str(root / "a,dst=/etc"), str(root / "a:b")):
        server, docker = _server(tmp_path / ("s" + str(abs(hash(bad)))))
        server.policy.allowed_mount_roots = (str(root),)
        with pytest.raises(HelperError) as e:
            server.handle(_engine(bad))
        assert e.value.code == "POLICY_DENIED" and docker.calls == [], bad

    server, docker = _server(tmp_path)
    server.handle(_engine(str(root / "abc")))
    argv = docker.calls[-1]
    assert "-v" not in argv
    assert argv[argv.index("--mount") + 1] == f"type=bind,source={(root / 'abc').resolve()},target=/model,readonly"
    assert argv[-2:] == ["--", "docker.io/vllm/vllm@sha256:" + "e" * 64]


# --- I2: replay window is bounded --------------------------------------------
def test_nonce_memory_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(helper_mod, "MAX_NONCES", 3)
    server, _ = _server(tmp_path, backend=FakeContainerBackend())
    for i in range(10):
        server.handle(_job(op=f"op{i}", job_key=f"job_{i}.0", ownership_marker=f"n:job_{i}.0:j"))
    assert len(server._seen_nonces) == 3


# --- reported host, logs and messages ----------------------------------------
@pytest.mark.parametrize("host", ["", "evil host", "x\nInjected: 1", "a" * 254, "-x.example", "a_b.example",
                                  "{{service_token}}"])
def test_reported_host_is_validated(host):
    with pytest.raises(ValidationError):
        Policy(engine_host=host)
    with pytest.raises(ValueError):
        JobSupervisor("n", FakeRuntime(), None, None, JobPolicy(), advertise_host=host)  # type: ignore[arg-type]


def test_job_bind_address_defaults_to_the_advertised_ip_and_must_be_an_ip():
    assert Policy(engine_host="10.0.0.5").effective_job_bind_address == "10.0.0.5"
    assert Policy(engine_host="gpu1.lab.example", job_bind_address="10.0.0.5").effective_job_bind_address == "10.0.0.5"
    with pytest.raises(ValidationError, match="job_bind_address"):
        Policy(engine_host="gpu1.lab.example", jobs_enabled=True)
    with pytest.raises(ValidationError):
        Config(control_plane_url="https://cp", policy={"jobs_enabled": True, "job_bind_address": "0.0.0.0 -p"})


def test_ipv6_service_endpoint_is_bracketed(tmp_path):
    led = ReservationLedger({}, range(8000, 8100), job_port_range=range(9000, 9010))
    sup = JobSupervisor("n", FakeRuntime(), Journal(tmp_path / "j.db"), led, JobPolicy(enabled=True,
                        allow_any_image=True), advertise_host="fd00::5")
    sup._executor = _Sync()
    j = JobAssignment(job_id="job_s", image=IMAGE, kind="service", port=9002)
    sup.reconcile({j.key: j})
    assert sup.observations()[0]["endpoint"] == "http://[fd00::5]:9002"


def test_log_tail_and_messages_are_bounded_valid_utf8(tmp_path):
    runtime = FakeRuntime()
    led = ReservationLedger({}, range(8000, 8100), job_port_range=range(9000, 9010))
    sup = JobSupervisor("n", runtime, Journal(tmp_path / "j.db"), led, JobPolicy(enabled=True, allow_any_image=True),
                        advertise_host="10.0.0.5")
    sup._executor = _Sync()
    j = JobAssignment(job_id="job_l", image=IMAGE)
    sup.reconcile({j.key: j})
    st = sup.state(j.key)
    assert st is not None and st.runtime_id is not None
    runtime.write_logs(st.runtime_id, "é" * 9000 + "\x00bad\udcff" + "end\n")
    st.message = "m" * 5000
    sup.reconcile({j.key: j})
    obs = sup.observations()[0]
    tail = obs["log_tail"].encode("utf-8")  # raises if not valid UTF-8
    assert len(tail) <= LOG_TAIL_BYTES and obs["log_tail"].endswith("�bad�end\n")
    assert obs["log_tail"].startswith("é")  # cut on a character boundary
    assert len(obs["message"].encode()) <= 1024
    assert jobs_mod.bounded_text("abé", 3, "head") == "ab"


# --- one bad job must not stall the node -------------------------------------
def test_an_invalid_job_fails_alone_and_the_rest_of_the_snapshot_applies():
    snap = DesiredSnapshot(node_id="n", generation=3, snapshot_hash="sha256:" + "0" * 64, jobs=[
        {"job_id": "job_bad", "replica": 1, "image": IMAGE, "env": {"NVIDIA_VISIBLE_DEVICES": "all"}},
        {"job_id": "job_ok", "replica": 0, "image": IMAGE},
        {"job_id": "x\ny", "replica": 0, "image": "nope"},  # unidentifiable: dropped, not echoed
    ])
    assert [j.job_id for j in snap.jobs] == ["job_ok"]
    assert [(b.job_id, b.replica) for b in snap.invalid_jobs] == [("job_bad", 1)]
    assert "reserved" in snap.invalid_jobs[0].message and "all" not in snap.invalid_jobs[0].message


# --- I3: metric label values are escaped -------------------------------------
def test_metric_label_values_are_escaped():
    from meridian_node.observability import _fmt

    assert _fmt((("model", 'a"b\\c\nd'),)) == 'model="a\\"b\\\\c\\nd"'


class _Sync:
    def submit(self, fn, *args):
        from concurrent.futures import Future

        fut: Future = Future()
        try:
            fut.set_result(fn(*args))
        except Exception as e:
            fut.set_exception(e)
        return fut
