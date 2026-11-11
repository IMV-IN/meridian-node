"""Batch jobs and services (DESIGN.md 25): lifecycle, exactly-once launch across
an agent restart, exclusive GPU reservation, local policy, the helper's
independent re-check, and the reconciler's observed-state report."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from engine_helpers import build_supervisor, make_assignment, make_signed_manifest
from pydantic import ValidationError

from meridian_node.helper import FakeContainerBackend, HelperError, HelperPolicy, HelperServer, respond
from meridian_node.jobs import JobPolicy, JobSupervisor
from meridian_node.journal import COMMITTED, STARTED, Journal
from meridian_node.models import DesiredPhase, DesiredSnapshot, JobAssignment
from meridian_node.models import JobPhase as P
from meridian_node.reconcile import Reconciler
from meridian_node.reservation import ReservationLedger
from meridian_node.runtime import FakeRuntime, HelperRuntime
from meridian_node.state import StateStore

IMAGE = "docker.io/pytorch/pytorch@sha256:" + "b" * 64
OPEN = JobPolicy(enabled=True, allow_any_image=True)


class Clock:
    def __init__(self):
        self.t = datetime(2026, 11, 10, 10, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += timedelta(seconds=seconds)


class SyncExecutor:
    """Runs a launch inline so a tick sees its result at once; the default executor is a thread pool."""

    def submit(self, fn, *args):
        fut: Future = Future()
        try:
            fut.set_result(fn(*args))
        except Exception as e:
            fut.set_exception(e)
        return fut


def job(job_id="job_a", replica=0, **over) -> JobAssignment:
    base = dict(job_id=job_id, replica=replica, image=IMAGE, args=["python", "train.py"], device_ids=["GPU-0"])
    return JobAssignment(**{**base, **over})


def ledger() -> ReservationLedger:
    return ReservationLedger(
        device_capacity={"GPU-0": 8 * 1024**3, "GPU-1": 8 * 1024**3},
        port_range=range(8000, 8100), job_port_range=range(9000, 9010),
    )


def make(tmp_path, policy=OPEN, runtime=None, led=None, clock=None, executor=None):
    runtime = runtime or FakeRuntime()
    led = led or ledger()
    clock = clock or Clock()
    sup = JobSupervisor("node_test", runtime, Journal(tmp_path / "journal.db"), led, policy, "10.0.0.5", now=clock,
                        executor=executor or SyncExecutor())
    return sup, runtime, led, clock


def desired(*jobs):
    return {j.key: j for j in jobs}


def test_job_runs_to_success_and_releases_its_gpu(tmp_path):
    sup, runtime, led, _ = make(tmp_path)
    j = job(env={"MERIDIAN_JOB_RANK": "0"})
    sup.reconcile(desired(j))
    st = sup.state(j.key)
    assert st.phase == P.RUNNING
    assert led.allocatable()["GPU-0"] == 0  # held exclusively
    req = runtime.job_request(st.runtime_id)
    assert req.image == IMAGE and req.args == ("python", "train.py") and req.env == {"MERIDIAN_JOB_RANK": "0"}

    runtime.write_logs(st.runtime_id, "epoch 1\n")
    sup.reconcile(desired(j))
    assert sup.observations()[0]["log_tail"] == "epoch 1\n"

    runtime.finish(st.runtime_id, 0, "done\n")
    sup.reconcile(desired(j))
    obs = sup.observations()[0]
    assert obs["phase"] == "Succeeded" and obs["exit_code"] == 0 and obs["log_tail"].endswith("done\n")
    assert led.allocatable()["GPU-0"] == 8 * 1024**3


def test_non_zero_exit_fails_without_restart(tmp_path):
    sup, runtime, _, _ = make(tmp_path)
    j = job()
    sup.reconcile(desired(j))
    runtime.finish(sup.state(j.key).runtime_id, 137)
    sup.reconcile(desired(j))
    sup.reconcile(desired(j))  # terminal: no relaunch
    st = sup.state(j.key)
    assert st.phase == P.FAILED and st.reason == "NonZeroExit" and st.exit_code == 137
    assert len(runtime.list_all()) == 1


def test_stop_cancels_and_removal_deletes_the_container(tmp_path):
    sup, runtime, led, _ = make(tmp_path)
    j = job()
    sup.reconcile(desired(j))
    rid = sup.state(j.key).runtime_id
    sup.reconcile(desired(j.model_copy(update={"desired_phase": DesiredPhase.STOPPED})))
    assert sup.state(j.key).phase == P.CANCELLED
    assert not runtime.inspect(rid).alive
    assert led.allocatable()["GPU-0"] > 0

    sup.reconcile({})  # dropped from the snapshot
    assert sup.state(j.key) is None and runtime.inspect(rid) is None


def test_removing_a_running_job_stops_it(tmp_path):
    sup, runtime, led, _ = make(tmp_path)
    j = job()
    sup.reconcile(desired(j))
    rid = sup.state(j.key).runtime_id
    sup.reconcile({})
    assert runtime.inspect(rid) is None and led.allocatable()["GPU-0"] > 0


def test_timeout_stops_the_job(tmp_path):
    sup, runtime, _, clock = make(tmp_path)
    j = job(timeout_seconds=60)
    sup.reconcile(desired(j))
    clock.advance(30)
    sup.reconcile(desired(j))
    assert sup.state(j.key).phase == P.RUNNING
    clock.advance(31)
    sup.reconcile(desired(j))
    st = sup.state(j.key)
    assert st.phase == P.FAILED and st.reason == "Timeout"
    assert not runtime.inspect(st.runtime_id).alive


def test_host_ceiling_applies_when_the_job_sets_no_timeout(tmp_path):
    sup, _, _, clock = make(tmp_path, policy=JobPolicy(enabled=True, allow_any_image=True, max_runtime_seconds=10))
    j = job()
    sup.reconcile(desired(j))
    clock.advance(11)
    sup.reconcile(desired(j))
    assert sup.state(j.key).reason == "Timeout"


@pytest.mark.parametrize(
    "policy,reason",
    [
        (JobPolicy(enabled=False), "disabled"),
        (JobPolicy(enabled=True), "image not allow-listed"),
        (JobPolicy(enabled=True, allow_any_image=True, allowed_registries=frozenset({"ghcr.io"})), "registry"),
    ],
)
def test_policy_denies_before_anything_runs(tmp_path, policy, reason):
    sup, runtime, _, _ = make(tmp_path, policy=policy)
    j = job()
    sup.reconcile(desired(j))
    st = sup.state(j.key)
    assert st.phase == P.FAILED and st.reason == "PolicyDenied" and reason in st.message
    assert runtime.list_all() == []


def test_allow_listed_digest_runs(tmp_path):
    policy = JobPolicy(enabled=True, allowed_image_digests=frozenset({"sha256:" + "b" * 64}))
    sup, _, _, _ = make(tmp_path, policy=policy)
    j = job()
    sup.reconcile(desired(j))
    assert sup.state(j.key).phase == P.RUNNING


def test_two_jobs_cannot_share_a_gpu_and_unknown_devices_are_rejected(tmp_path):
    sup, runtime, _, _ = make(tmp_path)
    a, b, c = job("job_a"), job("job_b"), job("job_c", device_ids=["GPU-9"])
    sup.reconcile(desired(a, b, c))
    assert sup.state(a.key).phase == P.RUNNING
    assert sup.state(b.key).reason == "LocalAdmissionRejected" and "in use" in sup.state(b.key).message
    assert sup.state(c.key).reason == "LocalAdmissionRejected" and "unknown device" in sup.state(c.key).message
    assert len(runtime.list_all()) == 1


def test_a_job_cannot_take_a_gpu_an_engine_holds(tmp_path):
    led = ledger()
    led.reserve("engine_1", ["GPU-0"], 4 * 1024**3, 8000)
    sup, _, _, _ = make(tmp_path, led=led)
    j = job()
    sup.reconcile(desired(j))
    assert sup.state(j.key).reason == "LocalAdmissionRejected"
    # and an engine cannot fit on a GPU a job holds
    k = job("job_k", device_ids=["GPU-1"])
    sup.reconcile(desired(j, k))
    assert led.allocatable()["GPU-1"] == 0


def test_service_reports_an_endpoint_and_port_must_be_in_range(tmp_path):
    sup, _, _, _ = make(tmp_path)
    svc = job("svc", kind="service", port=9001)
    bad = job("bad", kind="service", port=8000, device_ids=["GPU-1"])
    sup.reconcile(desired(svc, bad))
    by_id = {o["job_id"]: o for o in sup.observations()}
    assert by_id["svc"]["endpoint"] == "http://10.0.0.5:9001"
    assert by_id["bad"]["phase"] == "Failed" and "job port range" in by_id["bad"]["message"]


def test_restart_adopts_a_committed_launch_and_never_relaunches(tmp_path):
    sup, runtime, led, _ = make(tmp_path)
    j = job()
    sup.reconcile(desired(j))
    rid = sup.state(j.key).runtime_id

    # Agent restarts: fresh supervisor + ledger, same journal and containers.
    sup2, _, led2, _ = make(tmp_path, runtime=runtime, led=ledger())
    sup2.reconcile(desired(j))
    assert sup2.state(j.key).phase == P.RUNNING and sup2.state(j.key).runtime_id == rid
    assert led2.allocatable()["GPU-0"] == 0
    assert len(runtime.list_all()) == 1

    # The job finished while a third agent life was starting: resolved from the journal.
    runtime.finish(rid, 0)
    sup3, _, _, _ = make(tmp_path, runtime=runtime, led=ledger())
    sup3.reconcile(desired(j))
    assert sup3.state(j.key).phase == P.SUCCEEDED
    assert len(runtime.list_all()) == 1


def test_uncertain_launch_fails_instead_of_running_twice(tmp_path):
    sup, runtime, _, _ = make(tmp_path)
    j = job()
    journal = Journal(tmp_path / "journal.db")
    # Simulate a crash after the launch was requested but before its result was recorded.
    sup.reconcile(desired(j))
    journal.mark(f"job:{j.job_id}:{j.replica}", STARTED)
    runtime.remove(sup.state(j.key).runtime_id)

    sup2, _, _, _ = make(tmp_path, runtime=runtime, led=ledger())
    sup2.reconcile(desired(j))
    st = sup2.state(j.key)
    assert st.phase == P.FAILED and st.reason == "LaunchUncertain"
    assert runtime.list_all() == []


def test_restart_mid_launch_then_desired_stopped_removes_the_unrecorded_container(tmp_path):
    sup, runtime, _, _ = make(tmp_path)
    j = job()
    sup.reconcile(desired(j))
    rid = sup.state(j.key).runtime_id
    engine_id = runtime.orphan("sha256:" + "e" * 64, 8000, marker="node_test:engine_1:launch:node_test:engine_1:1")
    # Crash after the launch was requested but before its result was recorded: STARTED, no result.
    with sqlite3.connect(tmp_path / "journal.db") as db:
        db.execute("UPDATE journal SET state = ?, result = NULL WHERE operation_id = ?",
                   (STARTED, f"job:{j.job_id}:{j.replica}"))

    sup2, _, _, _ = make(tmp_path, runtime=runtime, led=ledger())
    sup2.reconcile(desired(j.model_copy(update={"desired_phase": DesiredPhase.STOPPED})))
    assert sup2.state(j.key).phase == P.CANCELLED
    assert runtime.inspect(rid) is None and runtime.inspect(engine_id).alive


def _restarted(tmp_path, runtime):
    """A fresh agent life: new supervisor and ledger, same journal and containers."""
    sup, _, led, _ = make(tmp_path, runtime=runtime, led=ledger())
    return sup, led


def test_restart_then_desired_stopped_stops_the_running_container(tmp_path):
    sup, runtime, _, _ = make(tmp_path)
    j = job()
    sup.reconcile(desired(j))
    rid = sup.state(j.key).runtime_id
    sup2, _ = _restarted(tmp_path, runtime)
    sup2.reconcile(desired(j.model_copy(update={"desired_phase": DesiredPhase.STOPPED})))
    assert sup2.state(j.key).phase == P.CANCELLED and not runtime.inspect(rid).alive


def test_restart_sweep_removes_a_dropped_job_and_spares_engines_and_desired_jobs(tmp_path):
    sup, runtime, _, _ = make(tmp_path)
    gone, kept = job("job_gone"), job("job_kept", device_ids=["GPU-1"])
    sup.reconcile(desired(gone, kept))
    gone_id, kept_id = sup.state(gone.key).runtime_id, sup.state(kept.key).runtime_id
    engine_id = runtime.orphan("sha256:" + "e" * 64, 8000, marker="node_test:engine_1:launch:node_test:engine_1:1")

    sup2, led2 = _restarted(tmp_path, runtime)
    sup2.reconcile(desired(kept))
    assert runtime.inspect(gone_id) is None
    assert runtime.inspect(kept_id).alive and runtime.inspect(engine_id).alive
    assert "GPU-0" in led2.free_devices() and "GPU-1" not in led2.free_devices()


def test_launch_does_not_block_the_tick_and_is_collected_later(tmp_path):
    release = threading.Event()

    class SlowRuntime(FakeRuntime):
        def launch_job(self, req):
            release.wait(10)
            return super().launch_job(req)

    runtime = SlowRuntime()
    sup, _, led, _ = make(tmp_path, runtime=runtime, executor=ThreadPoolExecutor(max_workers=1))
    j = job()
    sup.reconcile(desired(j))  # returns while the "pull" is still blocked
    assert sup.state(j.key).phase == P.STARTING and runtime.list_all() == []
    assert led.allocatable()["GPU-0"] == 0
    assert sup.journal.inspect("job:job_a:0").state == STARTED

    release.set()
    sup._launches[j.key].result(timeout=10)
    sup.reconcile(desired(j))
    st = sup.state(j.key)
    assert st.phase == P.RUNNING and runtime.inspect(st.runtime_id).alive
    assert sup.journal.inspect("job:job_a:0").state == COMMITTED


def test_cancel_during_launch_waits_and_stops_the_container(tmp_path):
    release = threading.Event()

    class SlowRuntime(FakeRuntime):
        def launch_job(self, req):
            release.wait(10)
            return super().launch_job(req)

    runtime = SlowRuntime()
    sup, _, led, _ = make(tmp_path, runtime=runtime, executor=ThreadPoolExecutor(max_workers=1))
    j = job()
    sup.reconcile(desired(j))
    assert sup.state(j.key).phase == P.STARTING
    threading.Timer(0.2, release.set).start()
    sup.reconcile(desired(j.model_copy(update={"desired_phase": DesiredPhase.STOPPED})))  # waits for the launch
    st = sup.state(j.key)
    assert st.phase == P.CANCELLED and not runtime.inspect(st.runtime_id).alive
    assert led.allocatable()["GPU-0"] > 0


def test_image_must_be_digest_pinned():
    for bad in ("pytorch/pytorch:latest", "docker.io/pytorch/pytorch:2.5", "alpine@sha256:" + "b" * 64, "x@sha256:zz"):
        with pytest.raises(ValidationError):
            job(image=bad)
    with pytest.raises(ValidationError):
        job(kind="daemon")


# --- helper: the root boundary re-checks everything -------------------------
def _helper(tmp_path, **over):
    policy = HelperPolicy(
        allowed_image_digests=frozenset(), allowed_devices=frozenset({"GPU-0"}),
        port_start=8000, port_stop=8100, allowed_mount_roots=(str(tmp_path),),
        **{"jobs_enabled": True, "job_allow_any_image": True, "job_port_start": 9000, "job_port_stop": 9010, **over},
    )
    backend = FakeContainerBackend()
    return HelperServer(backend, Journal(tmp_path / "helper.db"), policy), backend


def _job_req(op="op1", **payload):
    base = {"engine_id": "job_a.0", "job_key": "job_a.0", "image": IMAGE, "args": ["python", "t.py"], "env": {},
            "device_ids": ["GPU-0"], "port": 0, "memory_limit_bytes": 0, "ownership_marker": "n:job_a.0:op"}
    return {"operation_id": op, "agent_session_id": "s", "nonce": op + "-n", "fencing_epoch": 1,
            "type": "launch_job", "payload": {**base, **payload}}


def test_helper_launches_a_job_idempotently(tmp_path):
    server, backend = _helper(tmp_path)
    r1 = server.handle(_job_req())
    r2 = server.handle(_job_req())
    assert r1["result"] == r2["result"] and r2["replayed"] is True
    assert len(backend.list_all()) == 1


@pytest.mark.parametrize(
    "policy,payload,needle",
    [
        ({"jobs_enabled": False}, {}, "disabled"),
        ({"job_allow_any_image": False}, {}, "image not allow-listed"),
        ({"allowed_job_registries": frozenset({"ghcr.io"})}, {}, "registry"),
        ({}, {"image": "docker.io/pytorch/pytorch:latest"}, "digest-pinned"),
        ({}, {"device_ids": ["GPU-7"]}, "device"),
        ({}, {"port": 22}, "port"),
        ({}, {"env": {"BAD-NAME": "x"}}, "environment variable name"),
        ({}, {"args": ["ok", 7]}, "args"),
        ({}, {"privileged": True}, "not permitted"),
        ({}, {"mounts": ["/:/host"]}, "not permitted"),
        ({}, {"network": "host"}, "not permitted"),
    ],
)
def test_helper_rejects_jobs_outside_policy(tmp_path, policy, payload, needle):
    server, backend = _helper(tmp_path, **policy)
    with pytest.raises(HelperError) as e:
        server.handle(_job_req(**payload))
    assert e.value.code == "POLICY_DENIED" and needle in e.value.message
    assert backend.list_all() == []


def test_helper_replies_with_an_error_instead_of_dying_on_a_backend_failure(tmp_path):
    server, backend = _helper(tmp_path)

    def boom(spec):
        raise subprocess.CalledProcessError(125, ["docker", "run", "-e", "SECRET=x"], stderr="manifest unknown\n")

    backend.launch_job = boom  # type: ignore[method-assign]
    resp = respond(server, json.dumps(_job_req()).encode())
    assert resp["ok"] is False and resp["code"] == "BACKEND_ERROR"
    assert "manifest unknown" in resp["message"] and "SECRET" not in resp["message"]
    assert respond(server, b"{not json")["code"] == "BACKEND_ERROR"
    assert respond(server, json.dumps(_job_req(op="op2", port=22)).encode())["code"] == "POLICY_DENIED"


def test_helper_only_touches_containers_it_launched(tmp_path):
    server, backend = _helper(tmp_path)
    foreign = {"runtime_id": "foreign", "alive": True, "image_digest": "x", "ownership_marker": "",
               "device_ids": [], "port": 0, "exit_code": None}
    backend._c["foreign"] = foreign

    def call(op, kind, rid):
        return server.handle({"operation_id": op, "agent_session_id": "s", "nonce": op, "fencing_epoch": 1,
                              "type": kind, "payload": {"runtime_id": rid}})

    for i, rid in enumerate(("foreign", "missing")):
        for kind in ("stop", "remove", "logs"):
            if kind == "logs" and rid == "missing":
                assert call(f"{kind}{i}", kind, rid)["result"] == ""
                continue
            with pytest.raises(HelperError) as e:
                call(f"{kind}{i}", kind, rid)
            assert e.value.code == "POLICY_DENIED"
    assert foreign["alive"] and "foreign" in backend._c

    ours = server.handle(_job_req())["result"]
    backend.job_logs[ours] = "hi"
    assert call("l", "logs", ours)["result"] == "hi"
    call("s", "stop", ours)
    assert not backend.inspect(ours)["alive"]
    call("r", "remove", ours)
    assert backend.inspect(ours) is None


def test_supervisor_drives_jobs_through_the_helper(tmp_path):
    server, backend = _helper(tmp_path)
    server.refresh_epoch(1)
    runtime = HelperRuntime(server.handle, session_id="s", fencing_epoch=1)
    led = ReservationLedger({"GPU-0": 8 * 1024**3}, range(8000, 8100), job_port_range=range(9000, 9010))
    sup = JobSupervisor("n", runtime, Journal(tmp_path / "agent.db"), led, OPEN, now=Clock(), executor=SyncExecutor())
    j = job()
    sup.reconcile(desired(j))
    rid = sup.state(j.key).runtime_id
    assert backend.job_specs[rid]["image"] == IMAGE

    backend.job_logs[rid] = "step 10\n"
    backend._c[rid].update(alive=False, exit_code=0)
    sup.reconcile(desired(j))
    assert sup.state(j.key).phase == P.SUCCEEDED and sup.state(j.key).log_tail == "step 10\n"
    sup.reconcile({})
    assert backend.list_all() == []


# --- reconciler: jobs ride the same snapshot as engines ----------------------
def _snapshot(generation, assignments, jobs):
    return DesiredSnapshot(
        node_id="node_test", generation=generation, snapshot_hash="sha256:" + "0" * 64,
        assignments=assignments, jobs=jobs,
    )


def _reconciler(tmp_path, clock_factory, with_jobs=True):
    clock = clock_factory()
    runtime = FakeRuntime()
    manifest, digest, keys, revision = make_signed_manifest(tmp_path / "src", {"model.bin": b"w"})
    sup = build_supervisor(tmp_path, runtime, manifest, keys, tmp_path / "src", clock)
    sup.reservations.job_port_range = range(9000, 9010)
    jobs = None
    if with_jobs:
        jobs = JobSupervisor("node_test", runtime, sup.journal, sup.reservations, OPEN, "10.0.0.5", now=clock)
    rec = Reconciler("node_test", sup, StateStore(tmp_path / "state.db"), now=clock, jobs=jobs)
    return rec, runtime, sup, make_assignment(manifest_digest=digest, revision=revision)


def test_reconciler_reports_jobs_and_capacity(tmp_path, clock_factory):
    rec, runtime, sup, engine = _reconciler(tmp_path, clock_factory)
    free = next(d for d in sup.reservations.device_capacity if d not in engine.resources.device_ids) \
        if len(sup.reservations.device_capacity) > 1 else None
    if free is None:
        sup.reservations.device_capacity["GPU-job"] = 8 * 1024**3
        free = "GPU-job"
    j = job(device_ids=[free])
    obs = rec.apply(_snapshot(1, [engine], [j]))
    assert obs["engines"][0]["engine_id"] == engine.engine_id
    assert obs["jobs"][0]["phase"] == "Running"
    assert obs["capacity"]["jobs_enabled"] is True and obs["capacity"]["advertise_host"] == "10.0.0.5"
    assert obs["capacity"]["allocatable_vram_bytes"][free] == 0

    obs = rec.apply(_snapshot(2, [engine], []))
    assert obs["jobs"] == []
    assert obs["capacity"]["allocatable_vram_bytes"][free] > 0


def test_reconciler_fails_jobs_when_the_host_does_not_run_them(tmp_path, clock_factory):
    rec, runtime, _, engine = _reconciler(tmp_path, clock_factory, with_jobs=False)
    obs = rec.apply(_snapshot(1, [], [job()]))
    assert obs["jobs"][0]["phase"] == "Failed" and obs["jobs"][0]["reason"] == "JobsDisabled"
    assert obs["capacity"]["jobs_enabled"] is False
    assert runtime.list_all() == []


def test_reconciler_observation_matches_the_wire_contract(tmp_path, clock_factory):
    """What the agent actually posts validates against contracts/v1."""
    import json
    from pathlib import Path

    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource

    contracts = Path(__file__).resolve().parents[1] / "contracts" / "v1"
    registry = Registry()
    for path in contracts.glob("*.schema.json"):
        registry = Resource.from_contents(json.loads(path.read_text())) @ registry
    validator = Draft202012Validator(
        {"$ref": "https://meridian.dev/contracts/v1/observed-state.schema.json"}, registry=registry
    )

    rec, runtime, sup, engine = _reconciler(tmp_path, clock_factory)
    sup.reservations.device_capacity["GPU-job"] = 8 * 1024**3
    obs = rec.apply(_snapshot(1, [engine], [job(device_ids=["GPU-job"])]))
    obs.update(agent_session_id="session_1", sequence=1)
    validator.validate(obs)
