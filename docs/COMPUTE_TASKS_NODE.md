# Compute tasks, phase 1: node agent and helper specification

Status: **binding** for the node side of Meridian Compute tasks, phase 1.

The master document is `Meridian/docs/internal/COMPUTE_TASKS_SPEC.md` (branch `feat/compute-tasks`). This file repeats everything the node engineer needs, with the same names and values. If the two disagree, the master wins and this file is fixed. The schema deltas are in `contracts/v1/` on this branch:

- `desired-state.schema.json`: `kind: "task"` and `$defs/task`;
- `observed-state.schema.json`: `capacity.task_protocols` and `jobs[].task.stage`;
- `common.schema.json`: new error codes;
- `task-manifest.schema.json`: new.

The golden fixtures are `tests/fixtures/desired_state_task.json`, `observed_state_task.json` and `task_manifest.json`. Add them to `GOLDEN` in `tests/test_contract.py`.

**What a task is.** A researcher's Python function, called from a laptop, runs in a container on one of the workspace's own nodes. The node:

1. downloads a source **bundle** (`tar.gz`) and an arguments **payload** from the control plane (CP);
2. verifies them by sha256;
3. extracts the bundle safely;
4. runs a fixed runner command in a hardened container;
5. collects exactly one small result file and uploads it.

The agent and the helper **never parse or interpret** the bundle's files, the payload, or the result. They are opaque bytes with a digest and a size. Tar extraction reads the archive *format* only, to lay out files.

---

## 1. Constants

Define these in `meridian_node/tasks.py` and use them everywhere.

| Name | Value |
|---|---|
| `SUPPORTED_TASK_PROTOCOLS` | `(1,)` |
| `MAX_BUNDLE_BYTES` | `20 * 1024 * 1024` (compressed) |
| `MAX_BUNDLE_UNPACKED_BYTES` | `64 * 1024 * 1024` (sum of regular-file sizes) |
| `MAX_BUNDLE_MEMBERS` | `10_000` (files plus directories) |
| `MAX_BUNDLE_PATH_BYTES` | `255` |
| `MAX_BUNDLE_DEPTH` | `32` |
| `BUNDLE_COMPONENT` | `^[A-Za-z0-9_.@+=,-][A-Za-z0-9 _.@+=,-]{0,254}$`, and not `.` or `..` |
| `MAX_PAYLOAD_BYTES` | `256 * 1024` |
| `MAX_RESULT_BYTES` | `1024 * 1024` |
| `MAX_MANIFEST_BYTES` | `4096` |
| `TASK_ARGV` | `("python3", "-I", "-B", "/meridian/in/bundle/runner/meridian_runner.py")` |
| `TASK_OUT_TMPFS_BYTES` | `2 * 1024 * 1024` |
| `TASK_OUT_TMPFS_INODES` | `16` |
| `TASK_MIN_FREE_BYTES` | `512 * 1024 * 1024` |
| `FETCH_RETRY_BACKOFF` | `(2, 4, 8, 16, 32)` seconds |
| `ENTRY_NAME` | `^[A-Za-z_][A-Za-z0-9_]{0,63}(\.[A-Za-z_][A-Za-z0-9_]{0,63}){0,15}$`, at most 200 characters, `re.fullmatch` with `re.ASCII`; no component matching `^__.*__$` |
| `CLIENT_PYTHON` | `^(3\.[0-9]{1,2})?$` |
| `DIGEST` | `^sha256:[0-9a-f]{64}$` |

Existing patterns are reused as they are: `JOB_ID`, `IMAGE_REF`, `DEVICE_ID`, `OWNERSHIP_MARKER`, `MOUNT_PATH`, `job_strings_error`.

---

## 2. Lifecycle (node view)

| Replica phase | `task.stage` reported | Node is doing |
|---|---|---|
| `Pending` | — | the replica appeared in the snapshot |
| `Starting` | `fetching` | checking disk, downloading and verifying the bundle and payload, extracting, writing `task.json` |
| `Starting` | `launching` | journal plan, reservation, helper `launch_job` (image pull) |
| `Running` | `running` | the container is alive; the run timeout is enforced |
| `Running` | `collecting` | the container exited; helper `collect_task_result` |
| `Running` | `uploading` | `PUT …/result` (retried until accepted) |
| `Succeeded` / `Failed` / `Cancelled` | `done` | terminal, reported **only after** the upload succeeded or was not needed |

**The CP's clock decides.** The CP fails the task if:

- the replica is still `Pending`/`Starting` 1800 s after placement (`StartDeadline`);
- the node is silent for 300 s (`NodeLost`);
- its own backstop expires (`timeout + 1800 + 600` s from first start).

The node enforces the task's `timeout_seconds` precisely, from container start. It stops the container, reports `Failed/Timeout`, and does **not** collect.

**Cancel:** the snapshot sets `desired_phase: Stopped`. The node stops the container and reports `Cancelled`, with no collection.

### 2.1 Failure reasons the node reports (`reason`)

| Reason | When |
|---|---|
| `NodeDiskFull` | less than `TASK_MIN_FREE_BYTES` free on the `state_dir` filesystem before the fetch |
| `BundleFetchFailed` | the download failed after the retries; or the CP answered `410 BLOB_UNAVAILABLE` or `403` |
| `BundleDigestMismatch` / `PayloadDigestMismatch` | the sha256 or size differs from the snapshot |
| `UnsafeBundle` | §6 refused the archive |
| `ResultMissing` | the out directory is empty after exit |
| `ResultInvalid` | anything other than exactly one regular, single-link, non-empty file named `result.json` or `error.json`; or `result.json` with a non-zero exit |
| `ResultTooLarge` | the file is larger than `MAX_RESULT_BYTES` |
| `ResultUploadRejected` | the CP refused the upload with a 4xx other than `409 TASK_FINISHED` |
| `Completed` (`Succeeded`), `NonZeroExit` (`Failed`, an `error.json` was uploaded) | normal outcomes |
| `PolicyDenied`, `InvalidSpec`, `LaunchFailed`, `LaunchUncertain`, `SpecChanged`, `ContainerLost`, `LocalAdmissionRejected`, `Timeout`, `Cancelled` | existing job reasons, unchanged |

`message` follows the existing bounds (1024 bytes). It MUST NOT contain bundle member names, payload bytes or result bytes. It names the rule that failed and, for an archive, the member index.

---

## 3. Contract

### 3.1 Desired state: a task replica

```json
{
  "job_id": "job_0123456789abcdef0123", "replica": 0, "kind": "task",
  "image": "docker.io/pytorch/pytorch@sha256:…",
  "args": ["python3", "-I", "-B", "/meridian/in/bundle/runner/meridian_runner.py"],
  "env": {"HOME": "/tmp", "MERIDIAN_JOB_ID": "job_…", "MERIDIAN_JOB_NAME": "thesis/fit",
          "MERIDIAN_JOB_RANK": "0", "MERIDIAN_JOB_REPLICAS": "1"},
  "device_ids": ["GPU-…"], "memory_limit_bytes": 0, "port": 0,
  "timeout_seconds": 3600, "desired_phase": "Running",
  "task": {
    "protocol": 1,
    "bundle":  {"digest": "sha256:<64 hex>", "size_bytes": 81234},
    "payload": {"digest": "sha256:<64 hex>", "size_bytes": 57},
    "entry":   {"module": "train", "qualname": "fit"},
    "client_python": "3.12"
  }
}
```

**`models.JobAssignment` changes** (it stays `extra="forbid"`):

- `kind` accepts `"task"`;
- new field `task: Optional[TaskSpec] = None`;
- a model validator enforces all of these:
  - `kind == "task"` ⇔ `task is not None`;
  - for a task, `args == list(TASK_ARGV)` and `port == 0`;
  - `task.protocol in SUPPORTED_TASK_PROTOCOLS`.

**`TaskSpec`** (`extra="forbid"`):

| Field | Type | Rule |
|---|---|---|
| `protocol` | int | — |
| `bundle` | `BlobRef` | `digest` matches `DIGEST`; `size_bytes` 1 to `MAX_BUNDLE_BYTES` |
| `payload` | `BlobRef` | `digest` matches `DIGEST`; `size_bytes` 1 to `MAX_PAYLOAD_BYTES` |
| `entry` | `{module, qualname}`, `extra="forbid"` | both match `ENTRY_NAME` plus the dunder rule |
| `client_python` | str | `CLIENT_PYTHON`; default `""` |

A violation becomes `Failed/InvalidSpec` through the existing `invalid_jobs` path. The rest of the snapshot is still applied.

**`env`** is generated by the CP and holds no user values. The existing `job_strings_error` rules apply unchanged.

### 3.2 Capability

`JobSupervisor.capacity()` adds `"task_protocols": [1]` **only** when `policy.jobs_enabled` and `policy.tasks_enabled` are both true, and the runtime implements the task operations (§7). Otherwise the key is omitted.

The CP places a task only on a node whose last observation lists the protocol. Older nodes never advertise it, so they never receive a task. An older CP never sends one.

### 3.3 Observation

Each task replica's report carries `"task": {"stage": "<stage>"}` (§2). Non-task replicas never include `task`. The result is never put in the observation.

### 3.4 CP endpoints the agent calls

The base is the node control URL, over mTLS with the node certificate (the CP refuses to enable tasks unless `require_mtls` is on). Every request carries these headers:

| Header | Value |
|---|---|
| `X-Meridian-Agent-Session` | the agent's current `agent_session_id` |
| `X-Meridian-Fencing-Epoch` | the current fencing epoch, decimal |
| (existing) client-cert header | as `HttpTransport` sends today |

| Method and path | Purpose |
|---|---|
| `GET /control/v1/nodes/{node_id}/tasks/{job_id}/replicas/{replica}/bundle` | bundle bytes |
| `GET /control/v1/nodes/{node_id}/tasks/{job_id}/replicas/{replica}/payload` | payload bytes |
| `PUT /control/v1/nodes/{node_id}/tasks/{job_id}/replicas/{replica}/result?kind=result\|error` | upload the collected file |

**GET responses:**

- `200`: the body is streamed, with headers `Content-Length`, `X-Meridian-Content-Digest: sha256:<hex>`, `Content-Type: application/octet-stream`.
- The node MUST verify the bytes against the **snapshot's** `digest` and `size_bytes`. The headers are only cross-checked: a header that disagrees with the snapshot is also a mismatch.

**PUT request:** headers `Content-Type: application/octet-stream` and `X-Meridian-Content-Digest: sha256:<hex>`; the body is 1 to `MAX_RESULT_BYTES` bytes.

| PUT response | Meaning for the agent |
|---|---|
| `201 {"stored": true}` | done |
| `200 {"stored": false}` | done (the same result was already stored, for example after an agent restart) |
| `409 TASK_FINISHED` | the CP no longer wants it (cancelled or failed). Stop and report anyway |
| `409 CONFLICT` | a different result is already stored → `ResultUploadRejected` |
| `400 DIGEST_MISMATCH`, `413 TOO_LARGE`, `415`, `400 INVALID_REQUEST` | a bug → `ResultUploadRejected` |
| network error or 5xx | retry on later ticks (§8) |

**Errors on any of the three** (contracts/v1 envelope):

| Error | Meaning |
|---|---|
| `403 TASKS_DISABLED` | the feature is off on the CP |
| `403 NOT_ASSIGNED` | the replica is not currently assigned to this node (one answer for every sub-case) |
| `409 STALE_AGENT_SESSION` | the session or epoch is not current, or the lease has expired |
| `401`/`403 NODE_NOT_AUTHORIZED` | certificate problem |
| `410 BLOB_UNAVAILABLE` | the CP lost the file |

The node never asks for a blob by digest. The CP finds the digest from the replica it assigned to this node, so a node can never fetch another node's or another workspace's data.

**`Transport` additions** (`control.py`), implemented in `HttpTransport` and `tests/fake_control_plane.py`:

```python
def fetch_task_blob(self, node_id: str, job_id: str, replica: int, kind: str,  # "bundle" | "payload"
                    dest: BinaryIO, max_bytes: int, session_id: str, epoch: int) -> tuple[str, int]:
    """Stream into dest; abort past max_bytes. Returns (sha256 "sha256:<hex>", size)."""

def upload_task_result(self, node_id: str, job_id: str, replica: int, kind: str,  # "result" | "error"
                       data: bytes, digest: str, session_id: str, epoch: int) -> dict: ...
```

`HttpTransport` uses `client.stream("GET", …)` and `iter_bytes()`, with a read timeout of 30 s and a total timeout of 300 s per download. Path segments come only from validated values (`job_id` matches `JOB_ID`, `replica` is an int, `kind` is a literal).

---

## 4. Policy (root-owned config, `config.Policy`)

| Field | Default | Rule |
|---|---|---|
| `tasks_enabled` | `false` | when true, `jobs_enabled` MUST be true (validator) |
| `task_uid` | `65534` | 1 to 2,147,483,647. MUST NOT be 0 |
| `task_gid` | `65534` | the same |
| `task_tmp_bytes` | `4294967296` | ≥ 67,108,864. Size of the container's `/tmp` tmpfs, charged to its memory cap |
| `task_shm_bytes` | `1073741824` | ≥ 67,108,864. `--shm-size` |
| `task_out_root` | `/var/lib/meridian-node-helper/task-out` | absolute, `MOUNT_PATH` grammar, and not inside `state_dir` |

`HelperPolicy` gains `tasks_enabled`, `task_uid`, `task_gid`, `task_tmp_bytes`, `task_shm_bytes`, `task_in_root` (`<state_dir>/tasks`) and `task_out_root`, filled by `cli._run_helper` from the config. It also gains the test-only `task_out_tmpfs: bool = True`; when it is false the helper creates a plain directory instead of mounting a tmpfs. This flag is not in `Policy`.

**systemd** (`deploy/systemd/meridian-node-helper.service`): add `StateDirectory=meridian-node-helper` and `StateDirectoryMode=0700`.

---

## 5. Layout

Every path is built only from `job_key = f"{job_id}.{replica}"`, which MUST `fullmatch` `JOB_ID`. Nothing from a bundle member name, the payload or the result is ever part of a host path outside the extraction tree.

```
<state_dir>/tasks/                       agent, 0755
  <job_key>/                             agent, 0755
    bundle.tar.gz.part                   0600; deleted after extraction
    in/                                  0755 → /meridian/in (read-only)
      bundle/code/…                      dirs 0755, files 0644
      bundle/runner/meridian_runner.py   0644
      args.json                          0644 (the verified payload)
      task.json                          0644 (the manifest, §9.2)
<task_out_root>/                         root, 0700
  <job_key>/                             tmpfs mountpoint → /meridian/out (read-write)
                                         size=2097152, nr_inodes=16, mode=0700,
                                         uid=task_uid, gid=task_gid, nosuid, nodev, noexec
```

The container user (`task_uid`) reads `in/` through its 0755/0644 "other" bits. Host parent-directory permissions do not apply inside a bind mount.

---

## 6. Fetch and safe extraction (agent, non-root)

**Why the agent extracts.** The parser of hostile input runs with the least privilege. The helper only mounts the result.

**Steps** (on the launch executor, never on the tick thread; stage `fetching`):

1. Check that `shutil.disk_usage(state_dir).free >= TASK_MIN_FREE_BYTES`, else `NodeDiskFull`.
2. `shutil.rmtree(<state_dir>/tasks/<job_key>)` if it exists (a leftover from a crash before launch), then create it.
3. Fetch the bundle into `bundle.tar.gz.part` (opened `O_CREAT|O_EXCL|O_NOFOLLOW`, 0600), with `max_bytes = task.bundle.size_bytes`. Verify that the digest equals `task.bundle.digest` and the size equals `size_bytes`, else `BundleDigestMismatch`.
4. Extract into `in/bundle/` (rules below), then delete the `.part`.
5. Fetch the payload into `in/args.json` the same way (`max_bytes = task.payload.size_bytes`), else `PayloadDigestMismatch`.
6. Write `in/task.json` (§9.2).
7. Hand over to the launch path (stage `launching`).

**Retries:** transport errors and 5xx are retried at most 5 times with `FETCH_RETRY_BACKOFF`, then `BundleFetchFailed`. `403`, `410` and digest mismatches fail at once. Every failure removes `<state_dir>/tasks/<job_key>/` and never launches.

**Extraction rules.** Never use `extractall` or `extract`.

| Rule | Detail |
|---|---|
| Reader | `tarfile.open(fileobj=f, mode="r|gz")`, iterating members once. Any `tarfile.TarError`, `OSError`, `EOFError` or `zlib.error` → `UnsafeBundle` |
| Types | only `isreg()` (REGTYPE/AREGTYPE) and `isdir()`. Everything else (symlink, hardlink, char/block device, FIFO, contiguous, GNU sparse) → reject |
| Name | decode as UTF-8 and require ASCII; no NUL; no `\`; not absolute; split on `/`; a trailing `/` on directories is allowed; no empty, `.` or `..` component; each component matches `BUNDLE_COMPONENT`; total ≤ `MAX_BUNDLE_PATH_BYTES`; depth ≤ `MAX_BUNDLE_DEPTH` |
| Tree | the first component is `code` or `runner`. Under `runner` the only allowed members are the directory `runner` and the regular file `runner/meridian_runner.py` |
| Duplicates | a name seen twice → reject |
| Counts | members ≤ `MAX_BUNDLE_MEMBERS`; the sum of `member.size` over regular files ≤ `MAX_BUNDLE_UNPACKED_BYTES`, checked from the header before writing **and** from the bytes actually copied |
| Directories | created by the agent with mode 0755 (implicit parents too). An existing entry with the same name that is not a directory → reject |
| Files | the parent directory fd is opened with `O_RDONLY|O_DIRECTORY|O_NOFOLLOW` (walked component by component from `in/bundle`); the file with `os.open(name, O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, 0o644, dir_fd=parent)`; copied in 64 KiB chunks |
| Ignored | tar mode, uid/gid, uname/gname, mtime, pax headers and xattrs. Nothing is chmod-ed from the archive |
| Completeness | at the end, `code/` and `runner/meridian_runner.py` exist |
| On violation | stop, `rmtree(<state_dir>/tasks/<job_key>)`, `Failed/UnsafeBundle`, message `"<rule> (member #<index>)"`. Never retried |

---

## 7. Helper

### 7.1 `launch_job` with a task

`JOB_FIELDS` gains `"task"`. `HelperRuntime.launch_job` sends `"task": {"protocol": 1}` for a task. `JobLaunchRequest` gains `task_protocol: int = 0`; it is part of the journal digest, so a changed spec is still `SpecChanged`.

`_validate_job` keeps every existing check and, when `task` is present:

1. `self.policy.tasks_enabled`, else `POLICY_DENIED`.
2. `task` is a dict with exactly the key `protocol`, whose value is an `int` in `SUPPORTED_TASK_PROTOCOLS`.
3. `args == list(TASK_ARGV)` and `port == 0`.
4. **In directory** = `task_in_root/<job_key>/in`, derived and never read from the request.
   - `lstat` shows a directory, not a symlink;
   - `realpath(in) == in`;
   - it is strictly inside `realpath(task_in_root)` by path components;
   - it matches `MOUNT_PATH`;
   - the regular files `in/args.json`, `in/task.json` and `in/bundle/runner/meridian_runner.py` exist (checked with `lstat`, not followed).

   Any failure → `POLICY_DENIED`.
5. **Out directory** = `task_out_root/<job_key>`.
   - Create `task_out_root` (0700, root) if it is missing; refuse if it is a symlink or not owned by root.
   - If `<job_key>` exists and is a mountpoint → `POLICY_DENIED` ("stale task output").
   - Otherwise `mkdir` it (0700), then run `subprocess.run(["mount", "-t", "tmpfs", "-o", f"size={TASK_OUT_TMPFS_BYTES},nr_inodes={TASK_OUT_TMPFS_INODES},mode=0700,uid={task_uid},gid={task_gid},nosuid,nodev,noexec", "tmpfs", "--", out], check=True)`.
   - With `task_out_tmpfs = False` (tests): `mkdir` 0700 plus `chown(task_uid, task_gid)` only.

The returned spec gains `task_in_dir`, `task_out_dir`, `task_uid`, `task_gid`, `task_tmp_bytes` and `task_shm_bytes`, all from the helper's own policy.

**Docker argv.** Factor it into the pure function `task_run_args(spec) -> list[str]` and unit-test it. A task's argv is the job argv (labels, `--cap-drop ALL`, `no-new-privileges`, pids, logs, `--network meridian-jobs`, memory and swap, OOM score, CPUs, GPUs, env-file) with these additions, and no `-p`:

```
--user <task_uid>:<task_gid>
--read-only
--tmpfs /tmp:rw,nosuid,nodev,mode=1777,size=<task_tmp_bytes>
--shm-size <task_shm_bytes>
--mount type=bind,source=<task_in_dir>,target=/meridian/in,readonly
--mount type=bind,source=<task_out_dir>,target=/meridian/out
--label dev.meridian.task=1
--entrypoint python3
-- <image> -I -B /meridian/in/bundle/runner/meridian_runner.py
```

`--entrypoint` takes `TASK_ARGV[0]`, and `TASK_ARGV[1:]` follow the image. Without the override, an image's `ENTRYPOINT` (tini in the Jupyter image, the API server in vLLM) would receive the runner arguments.

**Container identity and filesystem:**

- The container runs as `task_uid:task_gid` (default `nobody`), never the image's user.
- Its root filesystem is **read-only**. The writable places are `/tmp` (tmpfs, charged to the memory cap), `/dev/shm`, and `/meridian/out` (2 MiB).
- The image is still allow-listed by `allowed_job_image_digests` or `job_allow_any_image`, and `allowed_job_registries`.

**Out directory: choice and rejected alternatives.**

- **Choice:** a helper-mounted host tmpfs per task. It has a hard size and inode limit and strict ownership, it never touches the host disk, and it outlives the container and agent or helper restarts.
- **Rejected:** `docker cp` out of the stopped container. It needs a writable in-image path (impossible with `--read-only` and a non-root user), puts a root-side parse of a container-made tar stream on the boundary, and loses the result if the container is removed first.
- **Rejected:** a plain directory bind, because it has no size limit.

### 7.2 `collect_task_result` (new read request; not journaled; safe to repeat)

Payload: `{"job_key": str, "runtime_id": str}`. Unknown keys → `INVALID_REQUEST`.

1. `job_key` must match `JOB_ID`; `runtime_id` must pass `_runtime_id`; the container must pass `_require_owned`; and `backend.inspect(runtime_id)["alive"]` must be false. Otherwise `POLICY_DENIED`.
2. `dfd = os.open(task_out_root/job_key, O_RDONLY|O_DIRECTORY|O_NOFOLLOW)`. `fstat(dfd)` must show `S_ISDIR` and `st_uid == task_uid`, else `POLICY_DENIED`.
3. `names = os.listdir(dfd)`:
   - `[]` → `{"kind": "none", "reason": "ResultMissing"}`;
   - not exactly one name in `{"result.json", "error.json"}` → `{"kind": "none", "reason": "ResultInvalid"}`.
4. `fd = os.open(name, O_RDONLY|O_NOFOLLOW|O_NONBLOCK, dir_fd=dfd)`. An `OSError` with `ELOOP` → `ResultInvalid`. `fstat(fd)` must show `S_ISREG`, `st_nlink == 1` and `0 < st_size <= MAX_RESULT_BYTES`, else `ResultInvalid` / `ResultTooLarge`.
5. Read at most `MAX_RESULT_BYTES + 1` bytes; more → `ResultTooLarge`.
6. Return `{"kind": "result"|"error", "size": n, "digest": "sha256:<hex>", "data_b64": base64.b64encode(data).decode()}`.

The helper never decodes the JSON. The response is at most about 1.4 MB on one line, and `unix_socket_send` already reads until the newline.

### 7.3 `remove` of a task container

When the container carries `dev.meridian.task=1`, `remove` takes the `job_key` from its `dev.meridian.job` label (which MUST `fullmatch` `JOB_ID`). After `docker rm`:

- if `task_out_root/<job_key>` is a mountpoint, run `["umount", "--", path]`;
- then `os.rmdir(path)`, ignoring `FileNotFoundError`.

The backend `inspect` needs to expose the labels (or `job_key`) for this. Extend `DockerContainerBackend.inspect` to return `"job_key": labels.get("dev.meridian.job", "")` and `"task": labels.get("dev.meridian.task") == "1"`.

### 7.4 Helper startup sweep

Before serving, for each entry `e` of `task_out_root`, check whether any container from `backend.list_all()` is a task with `job_key == e`. If none is:

- unmount it if it is a mountpoint;
- `rmdir` it (or `unlink` it if it is not a directory).

Names that fail `JOB_ID` are removed the same way, because the root belongs to the helper alone.

---

## 8. Agent: launch, collection, upload

`JobSupervisor` handles `kind == "task"` on the same path as jobs, with these differences:

**`_start`:**

- If the journal has no `launch_job` operation for this replica, submit `prepare(job)` (§6) to the executor and stay in `Starting` with stage `fetching`.
- When the future completes, continue with the existing plan, reserve and launch path (stage `launching`).
- After an agent restart with a `COMMITTED`/`STARTED` journal entry, skip `prepare` and use the existing adoption logic.
- `policy.check` additionally denies a task when `tasks_enabled` is off (`PolicyDenied`).

**`_observe`, when the container is not alive:**

| Step | stage | Action |
|---|---|---|
| 1 | `collecting` | `runtime.collect_task_result(job_key, runtime_id)` |
| 2 | `uploading` | if `kind` ∈ `result|error`: base64-decode, recompute the sha256 (it must equal `digest`, else `ResultInvalid`), and submit `transport.upload_task_result` to the executor |
| 3 | `done` | report the terminal phase |

While the upload future is pending, the phase stays `Running`. A network error or 5xx is resubmitted on a later tick with no fixed retry limit: the CP's deadlines bound it. The upload MUST be accepted (`201`/`200`) or answered `409 TASK_FINISHED` **before** the terminal phase is reported.

**Terminal phase:**

| Collected | Exit code | Phase / reason |
|---|---|---|
| `result` | 0 | `Succeeded/Completed` |
| `error` | any | `Failed/NonZeroExit` |
| `result` | ≠ 0 | `Failed/ResultInvalid`, no upload |
| `none` | any | `Failed/<reason>`, with `exit_code` kept |
| any | — | `ResultUploadRejected` if the CP refused the upload |

**Timeout and cancel:** the existing stop. The phase becomes `Failed/Timeout` or `Cancelled`. There is no collection and no upload.

**`Runtime` protocol additions** (`runtime.py`): `collect_task_result(job_key, runtime_id) -> dict`, implemented by `HelperRuntime` (request type `collect_task_result`, operation id `collect:<job_key>`) and by `FakeRuntime`.

**`FakeContainerBackend(run_tasks: bool = False)`:** when true, `launch_job` for a spec with `task_in_dir` runs this synchronously, with a 60 s timeout:

```
subprocess.run([sys.executable, "-I", "-B", f"{in}/bundle/runner/meridian_runner.py", "--in", in, "--out", out])
```

It records the exit code and marks the container not alive. This is what the cross-repo e2e test uses.

---

## 9. What the container sees (the runner contract; the node does not interpret it)

### 9.1 Command and environment

| Item | Value |
|---|---|
| Process | `python3 -I -B /meridian/in/bundle/runner/meridian_runner.py` |
| User | `task_uid:task_gid` |
| Environment | the snapshot `env` (`HOME=/tmp`, `MERIDIAN_JOB_*`) plus the helper's GPU variables. No user values and no credentials |
| Working directory | the runner `chdir`s to `/meridian/in/bundle/code` itself |

### 9.2 `/meridian/in/task.json` (the agent writes it)

The schema is `contracts/v1/task-manifest.schema.json`. Write it with `json.dumps({"protocol": t.protocol, "job_id": job.job_id, "entry": {"module": …, "qualname": …}, "client_python": t.client_python})`, after re-validating every value. It is at most `MAX_MANIFEST_BYTES`, with mode 0644.

### 9.3 Files and exit codes

| Path | Writer | Content |
|---|---|---|
| `/meridian/in/bundle/code/…` | agent (extraction) | the user's source tree |
| `/meridian/in/bundle/runner/meridian_runner.py` | agent (extraction) | the runner, shipped in the bundle by the SDK |
| `/meridian/in/args.json` | agent (verified payload) | JSON arguments (opaque to the node) |
| `/meridian/in/task.json` | agent | the manifest |
| `/meridian/out/result.json` *or* `/meridian/out/error.json` | runner | exactly one file, ≤ 1 MiB. It is written as `.partial` and renamed, so an interrupted write leaves `.partial` (→ `ResultInvalid`) |

**Runner exit codes:**

| Code | Meaning |
|---|---|
| 0 | `result.json` written |
| 1 | `error.json` written |
| 3 | neither could be written |
| 2 | Python could not start the runner |
| 137 | killed (memory or SIGKILL) |

The node uses only the §8 table.

---

## 10. Cleanup and crash recovery

| Event | Behaviour |
|---|---|
| Replica dropped from the snapshot (`_forget`) | stop and remove the container (existing); helper `remove` unmounts and removes the out directory (§7.3); the agent `rmtree`s `<state_dir>/tasks/<job_key>/` |
| Each reconcile (agent sweep) | every entry under `<state_dir>/tasks/` that is not the key of a desired replica is removed (`shutil.rmtree` for directories, which is symlink-safe; `unlink` for anything else) |
| Agent restart while `fetching` | no journal entry, so `prepare` runs again from scratch (step 2 removes the leftovers) |
| Agent restart after launch | existing journal adoption; an exited container is collected and uploaded; the out tmpfs is intact |
| Agent restart after upload, before reporting | it collects and uploads again (the CP answers `200 {"stored": false}`), then reports |
| Helper restart | journal replay as today; the startup sweep (§7.4) keeps the out directories of existing containers |
| Host reboot | the tmpfs and the container are gone → `ContainerLost` or `ResultMissing` |

---

## 11. Tests (each must exist under this name)

| # | Test | Asserts |
|---|---|---|
| N1 | `tests/test_task_extract.py::test_adversarial_archives_refused` (parametrised) | refused, with nothing left on disk: symlink, hardlink, absolute path, `../x`, `code/../../x`, char device, FIFO, duplicate member, member outside `code/`/`runner/`, extra file under `runner/`, 10,001 members, unpacked > 64 MiB (a header that lies small, and real bytes), a path of 256 bytes, a component with `\`, NUL or a non-ASCII character, a non-gzip input, a truncated gzip |
| N2 | `…::test_extract_normalises_modes` | a setuid 0o4755 file is written 0o644, and the uid and mtime are ignored |
| N3 | `tests/test_tasks.py::test_bundle_digest_mismatch_never_launches` | a wrong digest or size → `BundleDigestMismatch`; `runtime.launch_job` is never called |
| N4 | `…::test_task_paths_from_job_key_only` | every path is derived from `job_key`; a `job_key` that fails `JOB_ID` never touches the disk |
| N5 | `tests/test_helper_tasks.py::test_helper_refuses_invalid_task_launch` | `args ≠ TASK_ARGV`, port ≠ 0, `task` with an extra key, protocol 2, `tasks_enabled` false, a missing `in/` file, a symlinked `in/` → `POLICY_DENIED` |
| N6 | `…::test_helper_task_mounts_derived_and_readonly` | the spec has `task_in_dir`/`task_out_dir` derived from `job_key`, and the in mount is `readonly` |
| N7 | `…::test_collect_refuses_unsafe_outputs` | symlink, hardlink, FIFO, a directory named `result.json`, both files, an unknown name, an empty file, 1 MiB + 1 → the right reason; an alive or unowned container → `POLICY_DENIED` |
| N8 | `…::test_task_run_args_hardened` | `task_run_args` contains `--user`, `--read-only`, the `/tmp` tmpfs, `--shm-size`, both `--mount`s, `--entrypoint python3`, `--network meridian-jobs`, and no env value on argv |
| N9 | `tests/test_tasks.py::test_upload_precedes_terminal_report_and_survives_restart` | no terminal phase is reported before the upload is accepted; a new `JobSupervisor` over the same journal re-uploads and the fake CP answers `stored: false` |
| N10 | `…::test_task_capability_requires_both_switches` | `task_protocols` is present only with `jobs_enabled` and `tasks_enabled` |
| N11 | `…::test_fetch_sends_session_headers` | the fake CP rejects a stale session or epoch with `409 STALE_AGENT_SESSION`, and the agent sends its current values |
| N12 | `tests/test_contract.py` `GOLDEN` | add `desired_state_task.json`, `observed_state_task.json` and `task_manifest.json` (against `task-manifest.schema.json`) |

**Cross-repo e2e:** `Meridian/tests/control/test_tasks_e2e.py` (master spec §11.1). It drives a real agent with `HttpTransport` and `HelperRuntime` → `HelperServer(FakeContainerBackend(run_tasks=True))`, with `task_out_tmpfs=False` and `task_uid`/`task_gid` set to the test user.

**Manual validation on a GPU host:** master spec §11.2. The node-side checks are:

- `mount | grep task-out` is empty after a task ends;
- `docker inspect` of a running task shows `User: 65534:65534`, `ReadonlyRootfs: true`, and the two mounts;
- writing 3 MiB into `/meridian/out` fails with ENOSPC.

---

## 12. Not in phase 1 (node side)

- Volumes and secrets.
- Per-task CPU or memory requests (`job_memory_default_bytes` applies).
- Shared extracted-bundle caches across replicas.
- Multi-replica tasks; restarts or retries.
- Egress filtering (DESIGN §25.6 still applies).
- Dependency installs.
- Running tasks for rentals or partners.
