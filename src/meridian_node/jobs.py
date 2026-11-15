"""Batch jobs and services (DESIGN.md 25).

A job replica is a digest-pinned container that runs to completion (`job`) or
until stopped (`service`). It is supervised next to engines but follows a
different contract:

- A job is launched at most once. A committed launch is never repeated, even
  after an agent restart: the journal resolves it to the container that ran, and
  an uncertain launch fails the replica instead of silently running training
  code twice.
- A job holds its GPUs exclusively through the same reservation ledger engines
  use, so the two can never over-commit a device.
- Stopping needs no drain authorization: a job has no in-flight user requests.
  Removing a replica from the desired snapshot stops and removes its container.

The local job policy is an independent constraint. The agent checks it here and
the root helper checks it again before touching Docker.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import threading
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Optional, Sequence

from .control import ControlError, Transport
from .journal import COMMITTED, OBSERVED, PLANNED, STARTED, Journal, JournalConflict
from .models import JOB_TERMINAL, DesiredPhase, InvalidJob, JobAssignment, JobPhase, url_host, valid_host
from .reservation import ReservationError, ReservationLedger
from .runtime import HelperRejected, JobLaunchRequest, Runtime
from .tasks import (
    MAX_RESULT_BYTES,
    SUPPORTED_TASK_PROTOCOLS,
    TaskFailure,
    digest_ok,
    prepare_task,
    remove_task_dir,
    sweep_tasks_dir,
)

LOG_TAIL_BYTES = 8192
# NUL and lone surrogates (undecodable bytes) are not text: shown as U+FFFD.
_NOT_TEXT = re.compile("[\x00\ud800-\udfff]")
# Bounds of what the node reports per replica (contracts/v1 observed-state).
REASON_BYTES = 256
MESSAGE_BYTES = 1024


def bounded_text(text: str, max_bytes: int, keep: str = "tail") -> str:
    """At most `max_bytes` of valid UTF-8, cut on a character boundary. Invalid
    sequences and NUL become U+FFFD; nothing else is removed (the console and
    CLI escape on display). `keep` picks the end that survives a cut."""
    data = _NOT_TEXT.sub("\ufffd", text).encode("utf-8")
    if len(data) > max_bytes:
        data = data[-max_bytes:] if keep == "tail" else data[:max_bytes]
    # "ignore" only drops the partial character left at the cut.
    return data.decode("utf-8", "ignore")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(d: Optional[datetime]) -> Optional[str]:
    return d.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if d else None


@dataclass(frozen=True)
class JobPolicy:
    """The jobs slice of the local policy (config.Policy)."""

    enabled: bool = False
    allow_any_image: bool = False
    allowed_image_digests: frozenset[str] = frozenset()
    allowed_registries: frozenset[str] = frozenset()
    max_runtime_seconds: int = 0
    max_memory_bytes: int = 256 * 1024**3
    tasks_enabled: bool = False

    def check(self, job: JobAssignment) -> Optional[str]:
        """Why this job may not run here, or None if it may."""
        if not self.enabled:
            return "jobs are disabled by this host's policy"
        if job.kind == "task" and not self.tasks_enabled:
            return "tasks are disabled by this host's policy"
        if job.memory_limit_bytes > self.max_memory_bytes:
            return f"memory limit above this host's maximum of {self.max_memory_bytes} bytes"
        if self.allowed_registries and job.registry not in self.allowed_registries:
            return f"registry not allow-listed: {job.registry}"
        if not self.allow_any_image and job.image_digest not in self.allowed_image_digests:
            return f"image not allow-listed: {job.image_digest}"
        return None


@dataclass
class JobState:
    key: str
    job_id: str
    replica: int
    kind: str = "job"
    port: int = 0
    phase: JobPhase = JobPhase.PENDING
    runtime_id: Optional[str] = None
    exit_code: Optional[int] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    reason: str = ""
    message: str = ""
    log_tail: str = ""
    timeout_seconds: int = field(default=0)
    stage: str = "fetching"  # tasks only (docs/COMPUTE_TASKS_NODE.md 2)
    # A task's collected result awaiting upload, and the outcome reported once it is accepted.
    upload: Optional[tuple[str, bytes, str]] = None
    outcome: Optional[tuple[JobPhase, str, str]] = None


# Upload and fetch answers worth repeating; anything else from the control plane is final.
_RETRY_CODES = frozenset({"CONTROL_UNAVAILABLE", "RATE_LIMITED", "STALE_AGENT_SESSION"})
_NONE_REASONS = frozenset({"ResultMissing", "ResultInvalid", "ResultTooLarge"})


class JobSupervisor:
    def __init__(
        self,
        node_id: str,
        runtime: Runtime,
        journal: Journal,
        reservations: ReservationLedger,
        policy: JobPolicy,
        advertise_host: str = "127.0.0.1",
        now=_utcnow,
        executor: Optional[Executor] = None,
        tasks_dir: Optional[Path] = None,
    ):
        if not valid_host(advertise_host):
            raise ValueError("advertise_host must be an IP address or a host name of at most 253 characters")
        self.node_id = node_id
        self.runtime = runtime
        self.journal = journal
        self.reservations = reservations
        self.policy = policy
        self.advertise_host = advertise_host
        self.now = now
        self.fencing_epoch = 0
        self.session_id = ""
        self.transport: Optional[Transport] = None
        self._states: dict[str, JobState] = {}
        # `docker run` pulls the image first, which can take minutes. Launches run here so a
        # tick (heartbeat included) never waits on one; tests pass a synchronous executor.
        self._executor = executor or ThreadPoolExecutor(max_workers=4, thread_name_prefix="job-launch")
        self._launches: dict[str, Future[str]] = {}
        # Tasks: <state_dir>/tasks (None = this agent runs no tasks), in-flight fetches and uploads.
        self.tasks_dir = tasks_dir
        self._prepares: dict[str, tuple[Future[None], threading.Event]] = {}
        self._uploads: dict[str, Future[dict]] = {}

    def state(self, key: str) -> Optional[JobState]:
        return self._states.get(key)

    def bind_session(self, transport: Transport, session_id: str, fencing_epoch: int) -> None:
        """The agent's current control-plane session (Reconciler.bind_session)."""
        self.transport, self.session_id, self.fencing_epoch = transport, session_id, fencing_epoch

    # --- reconcile ------------------------------------------------------
    def reconcile(self, desired: dict[str, JobAssignment]) -> None:
        """Advance every replica by at most one bounded step, idempotently."""
        self._sweep(desired)
        if self.tasks_dir is not None:
            sweep_tasks_dir(self.tasks_dir, set(desired))
        for key in [k for k in self._states if k not in desired]:
            self._forget(self._states[key])
        for key, job in desired.items():
            st = self._states.get(key)
            if st is None:
                st = self._states[key] = JobState(
                    key=key, job_id=job.job_id, replica=job.replica, kind=job.kind, port=job.port,
                    timeout_seconds=job.timeout_seconds, stage="fetching" if job.kind == "task" else "",
                )
            self._step(st, job)

    def _step(self, st: JobState, job: JobAssignment) -> None:
        if st.phase in JOB_TERMINAL:
            return
        if job.desired_phase != DesiredPhase.RUNNING:
            self._cancel(st)
            return
        if st.phase == JobPhase.PENDING:
            self._start(st, job)
        elif st.phase == JobPhase.STARTING:
            if st.key in self._prepares:
                self._prepared(st, job)
            else:
                self._collect(st)
        elif st.phase == JobPhase.RUNNING:
            self._observe(st)

    def _sweep(self, desired: dict[str, JobAssignment]) -> None:
        """After an agent restart `_states` is empty. Stop and remove any running job container
        of this node whose replica the control plane no longer lists (DESIGN.md 25.2)."""
        for s in self.runtime.list_all():
            node, _, rest = s.ownership_marker.partition(":")
            key, _, operation_id = rest.partition(":")
            if node == self.node_id and operation_id.startswith("job:") and key not in desired:
                try:
                    self.runtime.stop(s.runtime_id, 10)
                    self.runtime.remove(s.runtime_id)
                except Exception:
                    pass  # retried on the next tick while the container is still listed

    def _owner(self, st: JobState) -> str:
        return f"job:{st.key}"

    def _op(self, st: JobState) -> str:
        return f"job:{st.job_id}:{st.replica}"

    def _start(self, st: JobState, job: JobAssignment) -> None:
        denied = self.policy.check(job)
        if denied is None and job.kind == "task" and not self.runs_tasks():
            denied = "this agent is not set up to run tasks"
        if denied:
            self._finish(st, JobPhase.FAILED, "PolicyDenied", denied)
            return
        if job.kind == "task" and self.journal.inspect(self._op(st)) is None:
            # Never launched in any agent life: fetch and extract first, off the tick.
            cancelled = threading.Event()
            self._prepares[st.key] = (self._executor.submit(self._prepare, job, cancelled), cancelled)
            st.phase, st.stage = JobPhase.STARTING, "fetching"
            self._prepared(st, job)
            return
        self._launch(st, job)

    def runs_tasks(self) -> bool:
        return self.tasks_dir is not None and callable(getattr(self.runtime, "collect_task_result", None))

    def _prepare(self, job: JobAssignment, cancelled: threading.Event) -> None:
        assert self.tasks_dir is not None

        def fetch(kind: str, dest: IO[bytes], max_bytes: int) -> tuple[str, int]:
            transport = self.transport  # read per attempt: the session may be re-established meanwhile
            if transport is None:
                raise ControlError("NO_SESSION", "no control-plane session")
            return transport.fetch_task_blob(
                self.node_id, job.job_id, job.replica, kind, dest, max_bytes, self.session_id, self.fencing_epoch
            )

        prepare_task(job, self.tasks_dir, fetch, cancelled)

    def _prepared(self, st: JobState, job: JobAssignment) -> None:
        """Continue to the launch once the fetch and extraction have finished."""
        fut, _ = self._prepares[st.key]
        if not fut.done():
            return
        del self._prepares[st.key]
        try:
            fut.result()
        except TaskFailure as e:
            self._finish(st, JobPhase.FAILED, e.reason, e.message)
            return
        except Exception as e:  # a bug, not the bundle: never echo it
            self._finish(st, JobPhase.FAILED, "BundleFetchFailed", f"could not prepare the task: {type(e).__name__}")
            return
        st.stage = "launching"
        self._launch(st, job)

    def _launch(self, st: JobState, job: JobAssignment) -> None:
        operation_id = self._op(st)
        marker = f"{self.node_id}:{st.key}:{operation_id}"
        req = JobLaunchRequest(
            job_key=st.key, image=job.image, args=tuple(job.args), env=dict(job.env),
            device_ids=tuple(job.device_ids), port=job.port,
            memory_limit_bytes=job.memory_limit_bytes, ownership_marker=marker,
            task_protocol=job.task.protocol if job.task is not None else 0,
        )
        digest = "sha256:" + hashlib.sha256(
            json.dumps(req.__dict__, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        try:
            prior = self.journal.plan(operation_id, digest, st.key, self.fencing_epoch, "launch_job")
        except JournalConflict:
            self._finish(st, JobPhase.FAILED, "SpecChanged", "a replica's spec cannot change after it was launched")
            return

        if prior in (COMMITTED, OBSERVED):
            # Launched in a previous agent life. Resolve to that container; never relaunch.
            row = self.journal.inspect(operation_id)
            st.runtime_id = row.result if row else None
            if st.runtime_id is None or self.runtime.inspect(st.runtime_id) is None:
                self._finish(st, JobPhase.FAILED, "ContainerLost", "the launched container no longer exists")
                return
            self._hold(st, job)
            st.phase, st.stage = JobPhase.RUNNING, "running"
            st.started_at = st.started_at or self.now()
            self._observe(st)
            return
        if prior == STARTED:
            # Crashed between asking for the launch and recording its result.
            adopted = next((s for s in self.runtime.list_all() if s.ownership_marker == marker), None)
            if adopted is None:
                self._finish(
                    st, JobPhase.FAILED, "LaunchUncertain",
                    "the agent restarted during launch and cannot prove whether the job ran; resubmit it",
                )
                return
            self.journal.mark(operation_id, COMMITTED, result=adopted.runtime_id)
            st.runtime_id = adopted.runtime_id
            self._hold(st, job)
            st.phase, st.stage, st.started_at = JobPhase.RUNNING, "running", self.now()
            return

        assert prior == PLANNED
        try:
            self.reservations.reserve_exclusive(self._owner(st), list(job.device_ids), job.port)
        except ReservationError as e:
            # Nothing was launched, so this op id is free to be retried by a resubmit.
            self._finish(st, JobPhase.FAILED, "LocalAdmissionRejected", str(e))
            return
        st.phase, st.stage = JobPhase.STARTING, "launching"
        self.journal.mark(operation_id, STARTED)
        self._launches[st.key] = self._executor.submit(self.runtime.launch_job, req)
        self._collect(st)

    def _collect(self, st: JobState, wait: bool = False) -> None:
        """Record the outcome of an in-flight launch once it has finished (or, with `wait`, when it does)."""
        fut = self._launches.get(st.key)
        if fut is None or not (wait or fut.done()):
            return
        del self._launches[st.key]
        try:
            st.runtime_id = fut.result()
        except Exception as e:  # helper rejected it or Docker failed
            self._finish(st, JobPhase.FAILED, "LaunchFailed", str(e)[:500])
            return
        self.journal.mark(self._op(st), OBSERVED, result=st.runtime_id)
        self.journal.mark(self._op(st), COMMITTED)
        st.phase, st.stage, st.started_at = JobPhase.RUNNING, "running", self.now()

    def _hold(self, st: JobState, job: JobAssignment) -> None:
        try:
            self.reservations.reserve_exclusive(self._owner(st), list(job.device_ids), job.port)
        except ReservationError:
            pass  # the container already exists; observing it matters more than the ledger

    def _observe(self, st: JobState) -> None:
        assert st.runtime_id is not None
        status = self.runtime.inspect(st.runtime_id)
        if status is None:
            self._finish(st, JobPhase.FAILED, "ContainerLost", "the job container disappeared")
            return
        st.log_tail = self._logs(st.runtime_id)
        if status.alive:
            limit = min((x for x in (st.timeout_seconds, self.policy.max_runtime_seconds) if x), default=0)
            if limit and st.started_at and (self.now() - st.started_at).total_seconds() > limit:
                self.runtime.stop(st.runtime_id, 10)
                st.log_tail = self._logs(st.runtime_id)
                self._finish(st, JobPhase.FAILED, "Timeout", f"exceeded the {limit}s runtime limit")
            return
        st.exit_code = status.exit_code
        if st.kind == "task":
            self._settle_task(st)
            return
        if status.exit_code == 0:
            self._finish(st, JobPhase.SUCCEEDED, "Completed", "")
        else:
            self._finish(st, JobPhase.FAILED, "NonZeroExit", f"exit code {status.exit_code}")

    # --- tasks: collection and upload (docs/COMPUTE_TASKS_NODE.md 8) -----
    def _settle_task(self, st: JobState) -> None:
        """The task container exited: collect its one result file, upload it,
        and only then report the terminal phase."""
        if st.upload is not None:
            self._uploaded(st)
            return
        assert st.runtime_id is not None
        st.stage = "collecting"
        try:
            res = self.runtime.collect_task_result(st.key, st.runtime_id)
        except HelperRejected as e:
            if e.code == "BACKEND_ERROR":
                return  # retried on the next tick
            # The output directory is gone or not the task user's (for example after a host reboot).
            self._finish(st, JobPhase.FAILED, "ResultMissing", "the task output could not be read")
            return
        except Exception:
            return  # the helper is unreachable; retried on the next tick
        exit_note = f"exit code {st.exit_code}"
        kind = res.get("kind")
        if kind == "none":
            reason = res.get("reason")
            self._finish(st, JobPhase.FAILED, reason if reason in _NONE_REASONS else "ResultInvalid", exit_note)
            return
        if kind not in ("result", "error"):
            self._finish(st, JobPhase.FAILED, "ResultInvalid", "the helper returned an unknown result kind")
            return
        if kind == "result" and st.exit_code != 0:
            self._finish(st, JobPhase.FAILED, "ResultInvalid", f"result.json written but {exit_note}")
            return
        data = _verified_result(res)
        if data is None:
            self._finish(st, JobPhase.FAILED, "ResultInvalid", "the collected result failed verification")
            return
        st.upload = (kind, data, res["digest"])
        st.outcome = (
            (JobPhase.SUCCEEDED, "Completed", "") if kind == "result" else (JobPhase.FAILED, "NonZeroExit", exit_note)
        )
        self._submit_upload(st)

    def _submit_upload(self, st: JobState) -> None:
        assert st.upload is not None
        st.stage = "uploading"
        self._uploads[st.key] = self._executor.submit(self._upload, st.job_id, st.replica, *st.upload)

    def _upload(self, job_id: str, replica: int, kind: str, data: bytes, digest: str) -> dict:
        transport = self.transport
        if transport is None:
            raise ControlError("CONTROL_UNAVAILABLE", "no control-plane session", retryable=True)
        return transport.upload_task_result(
            self.node_id, job_id, replica, kind, data, digest, self.session_id, self.fencing_epoch
        )

    def _uploaded(self, st: JobState) -> None:
        fut = self._uploads.get(st.key)
        if fut is None:
            self._submit_upload(st)
            return
        if not fut.done():
            return
        del self._uploads[st.key]
        try:
            fut.result()
        except ControlError as e:
            if e.code == "TASK_FINISHED":
                pass  # the control plane no longer wants it; report anyway
            elif e.retryable or e.code in _RETRY_CODES:
                self._submit_upload(st)
                return
            else:
                self._finish(st, JobPhase.FAILED, "ResultUploadRejected", f"the control plane refused it: {e.code}")
                return
        except Exception:
            self._submit_upload(st)  # a network error: retried until the control plane's deadlines stop it
            return
        assert st.outcome is not None
        self._finish(st, *st.outcome)

    def _cancel(self, st: JobState) -> None:
        self._teardown(st, remove=False)
        if st.phase not in JOB_TERMINAL:  # a launch that failed while awaited is already Failed
            self._finish(st, JobPhase.CANCELLED, "Cancelled", "")

    def _teardown(self, st: JobState, remove: bool) -> None:
        """Stop (and optionally remove) the replica's container. An in-flight launch is awaited so
        its container is not leaked; after an agent restart the id comes from the journal."""
        self._collect(st, wait=True)
        prepare = self._prepares.pop(st.key, None)
        if prepare is not None:
            prepare[1].set()  # the fetch stops at its next chunk and removes its directory
        self._uploads.pop(st.key, None)  # no upload after a cancel; a request in flight is answered harmlessly
        if st.runtime_id is None:
            row = self.journal.inspect(self._op(st))
            st.runtime_id = row.result if row else None
        if st.runtime_id is None:
            # Crashed mid-launch: the journal has no result, but the container may be running.
            marker = f"{self.node_id}:{st.key}:{self._op(st)}"
            orphan = next((s for s in self.runtime.list_all() if s.ownership_marker == marker), None)
            if orphan is not None:
                st.runtime_id, remove = orphan.runtime_id, True
        if st.runtime_id is None or self.runtime.inspect(st.runtime_id) is None:
            return
        if st.phase not in JOB_TERMINAL:
            self.runtime.stop(st.runtime_id, 10)
            st.log_tail = self._logs(st.runtime_id)
        if remove:
            self.runtime.remove(st.runtime_id)

    def _finish(self, st: JobState, phase: JobPhase, reason: str, message: str) -> None:
        st.phase, st.reason, st.message, st.stage = phase, reason, message, "done"
        st.upload = None
        st.finished_at = self.now()
        self.reservations.release(self._owner(st))

    def _forget(self, st: JobState) -> None:
        """The control plane dropped this replica: stop it and remove the container."""
        try:
            self._teardown(st, remove=True)
        except Exception:
            pass  # best effort; a leftover exited container holds no GPU
        if st.kind == "task" and self.tasks_dir is not None:
            try:
                remove_task_dir(self.tasks_dir, st.key)
            except OSError:
                pass  # the per-reconcile sweep retries
        self.reservations.release(self._owner(st))
        del self._states[st.key]

    def _logs(self, runtime_id: str) -> str:
        try:
            return bounded_text(self.runtime.logs(runtime_id, LOG_TAIL_BYTES), LOG_TAIL_BYTES)
        except Exception:
            return ""

    # --- report ---------------------------------------------------------
    def observations(self) -> list[dict]:
        out = []
        for st in self._states.values():
            endpoint = ""
            if st.kind == "service" and st.port and st.phase == JobPhase.RUNNING:
                endpoint = f"http://{url_host(self.advertise_host)}:{st.port}"
            out.append({
                "job_id": st.job_id, "replica": st.replica, "phase": st.phase.value,
                "exit_code": st.exit_code, "reason": bounded_text(st.reason, REASON_BYTES, "head"),
                "message": bounded_text(st.message, MESSAGE_BYTES, "head"),
                "started_at": _iso(st.started_at), "finished_at": _iso(st.finished_at),
                "endpoint": endpoint, "log_tail": st.log_tail,
                **({"task": {"stage": st.stage}} if st.kind == "task" else {}),
            })
        return out

    def capacity(self) -> dict:
        return {
            "jobs_enabled": self.policy.enabled,
            "advertise_host": self.advertise_host,
            "free_devices": self.reservations.free_devices(),
            "job_port_range": [self.reservations.job_port_range.start, self.reservations.job_port_range.stop],
            "free_job_ports": self.reservations.free_job_ports(),
            "running_jobs": sum(1 for s in self._states.values() if s.phase == JobPhase.RUNNING),
            # The control plane places a task only where its protocol is listed (docs/COMPUTE_TASKS_NODE.md 3.2).
            **({"task_protocols": list(SUPPORTED_TASK_PROTOCOLS)}
               if self.policy.enabled and self.policy.tasks_enabled and self.runs_tasks() else {}),
        }


def rejected_observations(
    jobs: Sequence[JobAssignment | InvalidJob], reason: str, message: str, now=_utcnow
) -> list[dict]:
    """Report every requested replica as Failed when this agent cannot run jobs
    at all, so the control plane never waits on a host that will not start them."""
    stamp = _iso(now())
    return [
        {
            "job_id": j.job_id, "replica": j.replica, "phase": JobPhase.FAILED.value, "exit_code": None,
            "reason": bounded_text(reason, REASON_BYTES, "head"),
            "message": bounded_text(message, MESSAGE_BYTES, "head"),
            "started_at": None, "finished_at": stamp,
            "endpoint": "", "log_tail": "",
            **({"task": {"stage": "done"}} if getattr(j, "kind", "") == "task" else {}),
        }
        for j in jobs
    ]


def _verified_result(res: dict) -> Optional[bytes]:
    """The collected bytes, if they match the size and digest the helper reported."""
    try:
        data = base64.b64decode(res.get("data_b64", ""), validate=True)
    except (binascii.Error, ValueError, TypeError):
        return None
    digest = res.get("digest")
    if not (0 < len(data) <= MAX_RESULT_BYTES) or res.get("size") != len(data) or not digest_ok(digest):
        return None
    return data if "sha256:" + hashlib.sha256(data).hexdigest() == digest else None
