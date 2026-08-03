"""Engine drivers (DESIGN.md 13).

A driver translates a typed Assignment into a validated LaunchRequest. It never
accepts a free-form command from the control plane: allowed parameters are an
explicit allow-list per driver, and unknown parameters are rejected. Each
driver also declares its readiness contract (probe path + expected served
model) so a live process is never reported Ready until it serves the requested
model.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import Assignment
from .runtime import LaunchRequest


class DriverError(ValueError):
    pass


@dataclass(frozen=True)
class ReadinessSpec:
    path: str
    served_model: str


class Driver(Protocol):
    kind: str

    def validate(self, assignment: Assignment, gpus: list[dict]) -> None: ...
    def build_launch(self, assignment: Assignment, artifact_path: str, ownership_marker: str) -> LaunchRequest: ...
    def readiness(self, assignment: Assignment) -> ReadinessSpec: ...


def _require_devices(assignment: Assignment, gpus: list[dict]) -> None:
    available = {g["device_id"] for g in gpus}
    for dev in assignment.resources.device_ids:
        if dev not in available:
            raise DriverError(f"device not present in inventory: {dev}")


def _check_params(params: dict, allowed: set[str]) -> None:
    unknown = set(params) - allowed
    if unknown:
        raise DriverError(f"unknown engine parameters rejected: {sorted(unknown)}")


class VllmDriver:
    kind = "vllm"
    _ALLOWED = {"served_model_name", "max_model_len", "tensor_parallel_size", "dtype", "gpu_memory_utilization"}

    def validate(self, assignment: Assignment, gpus: list[dict]) -> None:
        if assignment.engine.kind != self.kind:
            raise DriverError(f"driver {self.kind} cannot handle engine kind {assignment.engine.kind}")
        _check_params(assignment.parameters, self._ALLOWED)
        if "served_model_name" not in assignment.parameters:
            raise DriverError("vllm requires served_model_name")
        if not assignment.resources.device_ids:
            raise DriverError("vllm requires at least one GPU device")
        _require_devices(assignment, gpus)

    def build_launch(self, assignment: Assignment, artifact_path: str, ownership_marker: str) -> LaunchRequest:
        p = assignment.parameters
        args = [
            "--model", artifact_path,
            "--served-model-name", str(p["served_model_name"]),
            "--port", str(assignment.resources.port),
        ]
        if "max_model_len" in p:
            args += ["--max-model-len", str(p["max_model_len"])]
        args += ["--tensor-parallel-size", str(p.get("tensor_parallel_size", 1))]
        if "dtype" in p:
            args += ["--dtype", str(p["dtype"])]
        if "gpu_memory_utilization" in p:
            args += ["--gpu-memory-utilization", str(p["gpu_memory_utilization"])]
        return LaunchRequest(
            engine_id=assignment.engine_id,
            image_digest=assignment.engine.image_digest,
            args=tuple(args),
            env={"NVIDIA_VISIBLE_DEVICES": ",".join(assignment.resources.device_ids)},
            device_ids=tuple(assignment.resources.device_ids),
            artifact_mount=artifact_path,
            port=assignment.resources.port,
            ownership_marker=ownership_marker,
            memory_limit_bytes=assignment.resources.memory_limit_bytes,
        )

    def readiness(self, assignment: Assignment) -> ReadinessSpec:
        return ReadinessSpec(path="/health", served_model=str(assignment.parameters["served_model_name"]))


class GenericOpenAIDriver:
    kind = "openai-generic"
    _ALLOWED = {"served_model_name", "extra_args"}

    def validate(self, assignment: Assignment, gpus: list[dict]) -> None:
        if assignment.engine.kind != self.kind:
            raise DriverError(f"driver {self.kind} cannot handle engine kind {assignment.engine.kind}")
        _check_params(assignment.parameters, self._ALLOWED)
        if "served_model_name" not in assignment.parameters:
            raise DriverError("openai-generic requires served_model_name")
        _require_devices(assignment, gpus)

    def build_launch(self, assignment: Assignment, artifact_path: str, ownership_marker: str) -> LaunchRequest:
        extra = assignment.parameters.get("extra_args", [])
        if not isinstance(extra, list) or not all(isinstance(x, str) for x in extra):
            raise DriverError("extra_args must be a list of strings")
        args = ["--model-path", artifact_path, "--port", str(assignment.resources.port), *extra]
        return LaunchRequest(
            engine_id=assignment.engine_id,
            image_digest=assignment.engine.image_digest,
            args=tuple(args),
            env={},
            device_ids=tuple(assignment.resources.device_ids),
            artifact_mount=artifact_path,
            port=assignment.resources.port,
            ownership_marker=ownership_marker,
            memory_limit_bytes=assignment.resources.memory_limit_bytes,
        )

    def readiness(self, assignment: Assignment) -> ReadinessSpec:
        return ReadinessSpec(path="/v1/models", served_model=str(assignment.parameters["served_model_name"]))


class OllamaDriver:
    """Ollama server (DESIGN.md 13, 24 P2). Serves an OpenAI-compatible API; the
    model tag and runtime knobs map to OLLAMA_* env, never a free-form command."""

    kind = "ollama"
    _ALLOWED = {"served_model_name", "keep_alive", "num_parallel"}

    def validate(self, assignment: Assignment, gpus: list[dict]) -> None:
        if assignment.engine.kind != self.kind:
            raise DriverError(f"driver {self.kind} cannot handle engine kind {assignment.engine.kind}")
        _check_params(assignment.parameters, self._ALLOWED)
        if "served_model_name" not in assignment.parameters:
            raise DriverError("ollama requires served_model_name")
        _require_devices(assignment, gpus)

    def build_launch(self, assignment: Assignment, artifact_path: str, ownership_marker: str) -> LaunchRequest:
        p = assignment.parameters
        env = {
            "OLLAMA_HOST": f"0.0.0.0:{assignment.resources.port}",
            "OLLAMA_MODELS": artifact_path,
            "NVIDIA_VISIBLE_DEVICES": ",".join(assignment.resources.device_ids),
        }
        if "keep_alive" in p:
            env["OLLAMA_KEEP_ALIVE"] = str(p["keep_alive"])
        if "num_parallel" in p:
            env["OLLAMA_NUM_PARALLEL"] = str(p["num_parallel"])
        return LaunchRequest(
            engine_id=assignment.engine_id,
            image_digest=assignment.engine.image_digest,
            args=("serve",),
            env=env,
            device_ids=tuple(assignment.resources.device_ids),
            artifact_mount=artifact_path,
            port=assignment.resources.port,
            ownership_marker=ownership_marker,
            memory_limit_bytes=assignment.resources.memory_limit_bytes,
        )

    def readiness(self, assignment: Assignment) -> ReadinessSpec:
        return ReadinessSpec(path="/v1/models", served_model=str(assignment.parameters["served_model_name"]))


class _OpenAIServerDriver:
    """Shared base for OpenAI-compatible engine servers (DESIGN.md 13). The engine
    image's entrypoint is the server; the driver only supplies validated flags and
    env — never a free-form command. Subclasses declare the kind, the parameter
    allow-list, the flag mapping, and the readiness path."""

    kind = ""
    _ALLOWED: set[str] = {"served_model_name"}
    _readiness_path = "/v1/models"

    def validate(self, assignment: Assignment, gpus: list[dict]) -> None:
        if assignment.engine.kind != self.kind:
            raise DriverError(f"driver {self.kind} cannot handle engine kind {assignment.engine.kind}")
        _check_params(assignment.parameters, self._ALLOWED)
        if "served_model_name" not in assignment.parameters:
            raise DriverError(f"{self.kind} requires served_model_name")
        if not assignment.resources.device_ids:
            raise DriverError(f"{self.kind} requires at least one GPU device")
        _require_devices(assignment, gpus)

    def _args(self, assignment: Assignment, artifact_path: str) -> list[str]:
        raise NotImplementedError

    def _env(self, assignment: Assignment) -> dict[str, str]:
        return {"NVIDIA_VISIBLE_DEVICES": ",".join(assignment.resources.device_ids)}

    def build_launch(self, assignment: Assignment, artifact_path: str, ownership_marker: str) -> LaunchRequest:
        return LaunchRequest(
            engine_id=assignment.engine_id,
            image_digest=assignment.engine.image_digest,
            args=tuple(self._args(assignment, artifact_path)),
            env=self._env(assignment),
            device_ids=tuple(assignment.resources.device_ids),
            artifact_mount=artifact_path,
            port=assignment.resources.port,
            ownership_marker=ownership_marker,
            memory_limit_bytes=assignment.resources.memory_limit_bytes,
        )

    def readiness(self, assignment: Assignment) -> ReadinessSpec:
        return ReadinessSpec(path=self._readiness_path, served_model=str(assignment.parameters["served_model_name"]))


class TgiDriver(_OpenAIServerDriver):
    """HuggingFace Text Generation Inference (OpenAI-compatible since 1.4)."""

    kind = "tgi"
    _ALLOWED = {"served_model_name", "max_input_tokens", "max_total_tokens", "num_shard", "quantize", "dtype"}
    _readiness_path = "/health"

    def _args(self, assignment: Assignment, artifact_path: str) -> list[str]:
        p = assignment.parameters
        args = ["--model-id", artifact_path, "--hostname", "0.0.0.0", "--port", str(assignment.resources.port)]
        for key, flag in (("num_shard", "--num-shard"), ("max_input_tokens", "--max-input-tokens"),
                          ("max_total_tokens", "--max-total-tokens"), ("quantize", "--quantize"),
                          ("dtype", "--dtype")):
            if key in p:
                args += [flag, str(p[key])]
        return args


class SglangDriver(_OpenAIServerDriver):
    """SGLang server (`python -m sglang.launch_server`, OpenAI-compatible)."""

    kind = "sglang"
    _ALLOWED = {"served_model_name", "tp_size", "dtype", "context_length", "mem_fraction_static"}
    _readiness_path = "/health"

    def _args(self, assignment: Assignment, artifact_path: str) -> list[str]:
        p = assignment.parameters
        args = ["--model-path", artifact_path, "--host", "0.0.0.0", "--port", str(assignment.resources.port),
                "--served-model-name", str(p["served_model_name"])]
        for key, flag in (("tp_size", "--tp"), ("dtype", "--dtype"), ("context_length", "--context-length"),
                          ("mem_fraction_static", "--mem-fraction-static")):
            if key in p:
                args += [flag, str(p[key])]
        return args


class LlamaCppDriver(_OpenAIServerDriver):
    """llama.cpp server (`llama-server`, OpenAI-compatible /v1)."""

    kind = "llamacpp"
    _ALLOWED = {"served_model_name", "ctx_size", "n_gpu_layers", "parallel"}
    _readiness_path = "/health"

    def _args(self, assignment: Assignment, artifact_path: str) -> list[str]:
        p = assignment.parameters
        args = ["-m", artifact_path, "--host", "0.0.0.0", "--port", str(assignment.resources.port),
                "--alias", str(p["served_model_name"])]
        for key, flag in (("ctx_size", "-c"), ("n_gpu_layers", "-ngl"), ("parallel", "-np")):
            if key in p:
                args += [flag, str(p[key])]
        return args


class LmdeployDriver(_OpenAIServerDriver):
    """LMDeploy (`lmdeploy serve api_server`, OpenAI-compatible)."""

    kind = "lmdeploy"
    _ALLOWED = {"served_model_name", "tp", "session_len", "cache_max_entry_count"}
    _readiness_path = "/v1/models"

    def _args(self, assignment: Assignment, artifact_path: str) -> list[str]:
        p = assignment.parameters
        args = [artifact_path, "--server-name", "0.0.0.0", "--server-port", str(assignment.resources.port),
                "--model-name", str(p["served_model_name"])]
        for key, flag in (("tp", "--tp"), ("session_len", "--session-len"),
                          ("cache_max_entry_count", "--cache-max-entry-count")):
            if key in p:
                args += [flag, str(p[key])]
        return args


_DRIVERS: dict[str, Driver] = {d.kind: d for d in (
    VllmDriver(), GenericOpenAIDriver(), OllamaDriver(),
    TgiDriver(), SglangDriver(), LlamaCppDriver(), LmdeployDriver(),
)}


def get_driver(kind: str) -> Driver:
    driver = _DRIVERS.get(kind)
    if driver is None:
        raise DriverError(f"unsupported engine kind: {kind}")
    return driver
