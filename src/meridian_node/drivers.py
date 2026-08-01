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


_DRIVERS: dict[str, Driver] = {d.kind: d for d in (VllmDriver(), GenericOpenAIDriver())}


def get_driver(kind: str) -> Driver:
    driver = _DRIVERS.get(kind)
    if driver is None:
        raise DriverError(f"unsupported engine kind: {kind}")
    return driver
