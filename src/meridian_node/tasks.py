"""Compute tasks, phase 1: the node agent's side (docs/COMPUTE_TASKS_NODE.md).

A task is a researcher's Python function. The control plane hands the node a
source bundle (tar.gz) and an arguments payload by digest and size; the agent
downloads both over its authenticated session, verifies them, extracts the
bundle, writes a small manifest and hands over to the ordinary job launch path.

Everything that arrives here is untrusted data: the bundle's member names and
bytes, the payload and, later, the result. The agent never interprets them. Tar
extraction reads the archive *format* only, to lay out regular files and
directories under a directory derived from validated ids, with fixed modes.
"""

from __future__ import annotations

import errno
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import stat
import tarfile
import threading
import zlib
from pathlib import Path
from typing import IO, TYPE_CHECKING, Callable, Optional, cast

if TYPE_CHECKING:
    from .models import JobAssignment

# --- constants (§1) ---------------------------------------------------------
SUPPORTED_TASK_PROTOCOLS = (1,)
MAX_BUNDLE_BYTES = 20 * 1024 * 1024  # compressed
MAX_BUNDLE_UNPACKED_BYTES = 64 * 1024 * 1024  # sum of regular-file sizes
MAX_BUNDLE_MEMBERS = 10_000  # files plus directories
MAX_BUNDLE_PATH_BYTES = 255
MAX_BUNDLE_DEPTH = 32
BUNDLE_COMPONENT = re.compile(r"[A-Za-z0-9_.@+=,-][A-Za-z0-9 _.@+=,-]{0,254}", re.ASCII)
MAX_PAYLOAD_BYTES = 256 * 1024
MAX_RESULT_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 4096
TASK_ARGV = ("python3", "-I", "-B", "/meridian/in/bundle/runner/meridian_runner.py")
TASK_OUT_TMPFS_BYTES = 2 * 1024 * 1024
TASK_OUT_TMPFS_INODES = 16
TASK_MIN_FREE_BYTES = 512 * 1024 * 1024
FETCH_RETRY_BACKOFF = (2, 4, 8, 16, 32)  # seconds
ENTRY_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}(\.[A-Za-z_][A-Za-z0-9_]{0,63}){0,15}", re.ASCII)
MAX_ENTRY_NAME = 200
_DUNDER = re.compile(r"__.*__", re.ASCII)
CLIENT_PYTHON = re.compile(r"(3\.[0-9]{1,2})?", re.ASCII)
DIGEST = re.compile(r"sha256:[0-9a-f]{64}", re.ASCII)

# Bounds on the decompressed tar stream, beyond the per-file caps above. tarfile
# reads a pax or GNU long-name header's data whole into memory, so the stream as
# a whole is capped (file bytes, plus at most 4 KiB of headers and padding per
# member, plus the end-of-archive padding). After the end-of-archive marker only
# zero padding may follow, at most 1 MiB of it.
MAX_TRAILING_BYTES = 1024 * 1024
MAX_BUNDLE_STREAM_BYTES = MAX_BUNDLE_UNPACKED_BYTES + MAX_BUNDLE_MEMBERS * 4096 + MAX_TRAILING_BYTES
_CHUNK = 64 * 1024

TASK_STAGES = ("fetching", "launching", "running", "collecting", "uploading", "done")


def entry_name_ok(value: object) -> bool:
    """A dotted Python identifier path with no dunder component (ENTRY_NAME)."""
    return (
        isinstance(value, str) and len(value) <= MAX_ENTRY_NAME and ENTRY_NAME.fullmatch(value) is not None
        and not any(_DUNDER.fullmatch(part) for part in value.split("."))
    )


def digest_ok(value: object) -> bool:
    return isinstance(value, str) and DIGEST.fullmatch(value) is not None


def task_dir(tasks_root: Path, job_key: str) -> Path:
    """`<state_dir>/tasks/<job_key>`: the only way a task path is ever built. The
    key must be a validated job key; nothing from a bundle, payload or result is
    ever part of a host path outside the extraction tree."""
    from .models import JOB_ID

    if not isinstance(job_key, str) or not JOB_ID.fullmatch(job_key):
        raise ValueError("invalid job key")
    return tasks_root / job_key


# --- failures ---------------------------------------------------------------
class TaskFailure(Exception):
    """The replica fails with `reason` (§2.1). `message` never carries bundle
    member names, payload bytes or result bytes."""

    def __init__(self, reason: str, message: str):
        super().__init__(f"{reason}: {message}")
        self.reason = reason
        self.message = message


class UnsafeBundle(TaskFailure):
    def __init__(self, rule: str, index: Optional[int] = None):
        super().__init__("UnsafeBundle", rule if index is None else f"{rule} (member #{index})")


# --- safe extraction (§6) ---------------------------------------------------
# Header errors tarfile swallows after the first member (not in the type stubs).
_LENIENT_HEADER_ERRORS: tuple[type[Exception], ...] = tuple(
    getattr(tarfile, n) for n in ("InvalidHeaderError", "TruncatedHeaderError", "EmptyHeaderError")
)
_SubsequentHeaderError: type[Exception] = getattr(tarfile, "SubsequentHeaderError")


class _StrictTarInfo(tarfile.TarInfo):
    """tarfile ends the archive silently at a corrupt or truncated header after
    the first one. Here only a zero block ends it; anything else is an error."""

    @classmethod
    def fromtarfile(cls, tarfile_: tarfile.TarFile) -> "_StrictTarInfo":
        try:
            return cast(_StrictTarInfo, super().fromtarfile(tarfile_))
        except _LENIENT_HEADER_ERRORS as e:
            raise _SubsequentHeaderError(str(e)) from None


class _CappedReader(io.RawIOBase):
    """The decompressed stream, refusing to yield more than `cap` bytes."""

    def __init__(self, inner: IO[bytes], cap: int):
        self._inner, self._cap, self.total = inner, cap, 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:  # type: ignore[override]
        data = self._inner.read(len(buffer))
        self.total += len(data)
        if self.total > self._cap:
            raise UnsafeBundle("archive larger than allowed when decompressed")
        buffer[: len(data)] = data
        return len(data)


def _member_parts(name: str, is_dir: bool) -> list[str]:
    """The validated path components of a member name, or UnsafeBundle."""
    if not name.isascii():
        raise UnsafeBundle("member name is not ASCII")
    if "\x00" in name or "\\" in name:
        raise UnsafeBundle("member name contains NUL or backslash")
    if name.startswith("/"):
        raise UnsafeBundle("absolute member name")
    if is_dir and name.endswith("/"):
        name = name[:-1]
    if len(name.encode()) > MAX_BUNDLE_PATH_BYTES:
        raise UnsafeBundle("member name too long")
    parts = name.split("/")
    if len(parts) > MAX_BUNDLE_DEPTH:
        raise UnsafeBundle("member path too deep")
    for part in parts:
        if part in ("", ".", ".."):
            raise UnsafeBundle("empty, '.' or '..' path component")
        if not BUNDLE_COMPONENT.fullmatch(part):
            raise UnsafeBundle("path component outside the allowed characters")
    if parts[0] not in ("code", "runner"):
        raise UnsafeBundle("member outside code/ and runner/")
    if parts[0] == "runner" and not (parts == ["runner"] and is_dir) and not (
        parts == ["runner", "meridian_runner.py"] and not is_dir
    ):
        raise UnsafeBundle("unexpected member under runner/")
    return parts


_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _open_dir(parent_fd: int, name: str, create: bool) -> int:
    """Open (and with `create`, make, mode 0755) one directory below `parent_fd`, never following a link."""
    created = False
    if create:
        try:
            os.mkdir(name, 0o755, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
    try:
        fd = os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
    except OSError as e:
        if e.errno in (errno.ENOTDIR, errno.ELOOP):
            raise UnsafeBundle("a directory's name is already used by a file") from None
        raise
    if created:
        os.fchmod(fd, 0o755)  # fixed mode whatever the umask; nothing comes from the archive
    return fd


def _open_parent(root_fd: int, parts: list[str]) -> int:
    """Walk from the extraction root to the parent of `parts`, creating directories as needed."""
    fd = os.dup(root_fd)
    try:
        for part in parts:
            nxt = _open_dir(fd, part, create=True)
            os.close(fd)
            fd = nxt
        return fd
    except BaseException:
        os.close(fd)
        raise


def extract_bundle(source: IO[bytes], dest: Path) -> None:
    """Extract a gzip-compressed tar from `source` into the existing, empty
    directory `dest` (`in/bundle`), by the rules of §6. Raises UnsafeBundle; the
    caller removes the whole task directory on failure."""
    root_fd = os.open(dest, _DIR_FLAGS)
    index = 0
    try:
        stream = _CappedReader(cast(IO[bytes], gzip.GzipFile(fileobj=source, mode="rb")), MAX_BUNDLE_STREAM_BYTES)
        with tarfile.open(fileobj=cast(IO[bytes], stream), mode="r|", tarinfo=_StrictTarInfo) as tf:
            seen: set[str] = set()
            unpacked = 0
            for index, member in enumerate(tf):
                if index >= MAX_BUNDLE_MEMBERS:
                    raise UnsafeBundle("too many members", index)
                is_dir = member.type == tarfile.DIRTYPE
                if not is_dir and (member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE) or member.issparse()):
                    raise UnsafeBundle("member is not a regular file or directory", index)
                try:
                    parts = _member_parts(member.name, is_dir)
                except UnsafeBundle as e:
                    raise UnsafeBundle(e.message, index) from None
                key = "/".join(parts)
                if key in seen:
                    raise UnsafeBundle("duplicate member", index)
                seen.add(key)
                if is_dir:
                    os.close(_open_parent(root_fd, parts))
                    continue
                unpacked += member.size
                if unpacked > MAX_BUNDLE_UNPACKED_BYTES:
                    raise UnsafeBundle("unpacked size above the limit", index)
                _write_member(tf, member, root_fd, parts, index)
            # Only zero padding may follow the end-of-archive marker.
            trailing = 0
            while chunk := cast(IO[bytes], tf.fileobj).read(_CHUNK):
                trailing += len(chunk)
                if trailing > MAX_TRAILING_BYTES or chunk.count(0) != len(chunk):
                    raise UnsafeBundle("data after the end of the archive")
        for required, want_dir in ((["code"], True), (["runner", "meridian_runner.py"], False)):
            try:
                st = os.stat("/".join(required), dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                raise UnsafeBundle("code/ or runner/meridian_runner.py missing") from None
            if stat.S_ISDIR(st.st_mode) != want_dir or not (want_dir or stat.S_ISREG(st.st_mode)):
                raise UnsafeBundle("code/ or runner/meridian_runner.py missing")
    except UnsafeBundle:
        raise
    except (tarfile.TarError, OSError, EOFError, zlib.error, ValueError) as e:
        # gzip.BadGzipFile is an OSError; the archive's own text is never echoed.
        raise UnsafeBundle(f"unreadable archive ({type(e).__name__})", index) from None
    finally:
        os.close(root_fd)


def _write_member(tf: tarfile.TarFile, member: tarfile.TarInfo, root_fd: int, parts: list[str], index: int) -> None:
    src = tf.extractfile(member)
    if src is None:
        raise UnsafeBundle("member is not a regular file or directory", index)
    parent = _open_parent(root_fd, parts[:-1])
    try:
        try:
            fd = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o644,
                         dir_fd=parent)
        except FileExistsError:
            raise UnsafeBundle("duplicate member", index) from None
        try:
            os.fchmod(fd, 0o644)  # setuid, setgid and exec bits from the archive are never applied
            copied = 0
            while chunk := src.read(_CHUNK):
                copied += len(chunk)
                if copied > member.size:
                    raise UnsafeBundle("member larger than its header", index)
                view = memoryview(chunk)
                while view:
                    view = view[os.write(fd, view):]
            if copied != member.size:
                raise UnsafeBundle("member shorter than its header", index)
        finally:
            os.close(fd)
    finally:
        os.close(parent)


# --- fetch and prepare (§6, steps 1-7) --------------------------------------
# fetch(kind, dest, max_bytes) -> (digest, size), with the current session headers (JobSupervisor).
Fetch = Callable[[str, IO[bytes], int], "tuple[str, int]"]


class _VerifyingWriter(io.RawIOBase):
    """Where a download is streamed: hashes and counts what is written, refuses
    more than `max_bytes`, and stops at once when the replica is cancelled."""

    def __init__(self, fd: int, max_bytes: int, cancelled: threading.Event, mismatch: str):
        self._fd, self._max, self._cancelled, self._mismatch = fd, max_bytes, cancelled, mismatch
        self.size = 0
        self.sha = hashlib.sha256()

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:  # type: ignore[override]
        if self._cancelled.is_set():
            raise TaskFailure("Cancelled", "cancelled during the download")
        self.size += len(data)
        if self.size > self._max:
            raise TaskFailure(self._mismatch, "more bytes than the snapshot's size_bytes")
        self.sha.update(data)
        view = memoryview(data)
        while view:
            view = view[os.write(self._fd, view):]
        return len(data)


_RETRY_CODES = {"CONTROL_UNAVAILABLE", "RATE_LIMITED", "STALE_AGENT_SESSION"}


def _download(fetch: Fetch, kind: str, path: Path, digest: str, size: int, mode: int,
              cancelled: threading.Event, sleep: Callable[[float], object]) -> None:
    """Fetch one blob into `path` (created O_EXCL|O_NOFOLLOW) and verify it against the snapshot."""
    from .control import ControlError

    mismatch = "BundleDigestMismatch" if kind == "bundle" else "PayloadDigestMismatch"
    for attempt in range(len(FETCH_RETRY_BACKOFF) + 1):
        if attempt:
            sleep(FETCH_RETRY_BACKOFF[attempt - 1])
            if cancelled.is_set():
                raise TaskFailure("Cancelled", "cancelled during the download")
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode)
        try:
            writer = _VerifyingWriter(fd, size, cancelled, mismatch)
            try:
                got_digest, got_size = fetch(kind, cast(IO[bytes], writer), size)
            except ControlError as e:
                if e.code == "DIGEST_MISMATCH":
                    raise TaskFailure(mismatch, f"the {kind} does not match the snapshot") from None
                if e.code in _RETRY_CODES or e.retryable:
                    continue
                raise TaskFailure("BundleFetchFailed", f"the control plane refused the {kind}: {e.code}") from None
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    raise TaskFailure("NodeDiskFull", f"no space left while writing the {kind}") from None
                raise TaskFailure("BundleFetchFailed", f"could not write the {kind}: {type(e).__name__}") from None
            ours = "sha256:" + writer.sha.hexdigest()
            # The snapshot decides; the transport's own count and digest are only cross-checked.
            if (ours, writer.size) != (digest, size) or (got_digest, got_size) != (digest, size):
                raise TaskFailure(mismatch, f"the {kind} does not match the snapshot's digest or size")
            os.fchmod(fd, mode)
            os.fsync(fd)
            return
        finally:
            os.close(fd)
    raise TaskFailure("BundleFetchFailed", f"the {kind} download failed after {len(FETCH_RETRY_BACKOFF)} retries")


def manifest_bytes(job: "JobAssignment") -> bytes:
    """`/meridian/in/task.json` (§9.2), from re-validated snapshot values only."""
    from .models import JOB_ID

    t = job.task
    if t is None:
        raise ValueError("not a task")
    if (
        t.protocol not in SUPPORTED_TASK_PROTOCOLS or not JOB_ID.fullmatch(job.job_id)
        or not entry_name_ok(t.entry.module) or not entry_name_ok(t.entry.qualname)
        or not CLIENT_PYTHON.fullmatch(t.client_python)
    ):
        raise TaskFailure("InvalidSpec", "task fields failed re-validation")
    data = json.dumps({
        "protocol": t.protocol, "job_id": job.job_id,
        "entry": {"module": t.entry.module, "qualname": t.entry.qualname}, "client_python": t.client_python,
    }).encode()
    if len(data) > MAX_MANIFEST_BYTES:
        raise TaskFailure("InvalidSpec", "task manifest too large")
    return data


def prepare_task(job: "JobAssignment", tasks_root: Path, fetch: Fetch, cancelled: threading.Event,
                 sleep: Optional[Callable[[float], object]] = None) -> None:
    """§6 steps 1-6, run on the launch executor. Any failure removes the task
    directory and raises TaskFailure; nothing is launched after one."""
    if job.task is None:
        raise ValueError("not a task")
    task = job.task
    base = task_dir(tasks_root, job.key)
    wait = sleep or cancelled.wait  # wakes early when cancelled
    try:
        tasks_root.mkdir(mode=0o755, parents=True, exist_ok=True)
        if shutil.disk_usage(tasks_root).free < TASK_MIN_FREE_BYTES:
            raise TaskFailure("NodeDiskFull", f"less than {TASK_MIN_FREE_BYTES} bytes free for the task")
        _remove(base)  # a leftover from a crash before launch
        for d in (base, base / "in", base / "in" / "bundle"):
            os.mkdir(d, 0o755)
            os.chmod(d, 0o755)
        part = base / "bundle.tar.gz.part"
        _download(fetch, "bundle", part, task.bundle.digest, task.bundle.size_bytes, 0o600, cancelled, wait)
        if cancelled.is_set():
            raise TaskFailure("Cancelled", "cancelled")
        with open(os.open(part, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC), "rb") as src:
            extract_bundle(src, base / "in" / "bundle")
        part.unlink()
        _download(fetch, "payload", base / "in" / "args.json", task.payload.digest, task.payload.size_bytes, 0o644,
                  cancelled, wait)
        fd = os.open(base / "in" / "task.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o644)
        with open(fd, "wb") as f:
            os.fchmod(fd, 0o644)
            f.write(manifest_bytes(job))
        if cancelled.is_set():
            raise TaskFailure("Cancelled", "cancelled")
    except BaseException as e:
        _remove(base)
        if isinstance(e, TaskFailure):
            raise
        if isinstance(e, OSError) and e.errno == errno.ENOSPC:
            raise TaskFailure("NodeDiskFull", "no space left while preparing the task") from None
        if isinstance(e, Exception):
            raise TaskFailure("BundleFetchFailed", f"could not prepare the task: {type(e).__name__}") from None
        raise


def _remove(path: Path) -> None:
    """Remove a directory tree (symlink-safe) or a single entry; absent is fine."""
    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()
    except FileNotFoundError:
        pass


def remove_task_dir(tasks_root: Path, job_key: str) -> None:
    _remove(task_dir(tasks_root, job_key))


def sweep_tasks_dir(tasks_root: Path, keep: set[str]) -> None:
    """Remove every entry under `<state_dir>/tasks/` that is not a desired replica's key (§10)."""
    try:
        names = os.listdir(tasks_root)
    except FileNotFoundError:
        return
    for name in names:
        if name not in keep:
            try:
                _remove(tasks_root / name)
            except OSError:
                pass  # retried on the next reconcile
