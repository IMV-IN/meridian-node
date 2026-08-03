# Engine drivers

A driver translates a typed `Assignment` into a validated launch request. It
never accepts a free-form command from the control plane: each driver declares
an explicit parameter allow-list and rejects anything else. See
[DESIGN.md](../DESIGN.md) section 13 and `src/meridian_node/drivers.py`.

## Contract

```text
validate(assignment, gpus)                  -> raises on unsupported/invalid spec
build_launch(assignment, artifact_path, marker) -> LaunchRequest (typed, no shell)
readiness(assignment)                       -> ReadinessSpec (probe path + model)
```

Readiness is a real check: an engine is not reported `Ready` until the requested
model is served, not merely because the process is alive.

## Supported drivers

| kind | status | allowed parameters |
|---|---|---|
| `vllm` | first-release | `served_model_name` (required), `max_model_len`, `tensor_parallel_size`, `dtype`, `gpu_memory_utilization` |
| `openai-generic` | supported | `served_model_name` (required), `extra_args` (list of strings) |
| `ollama` | supported | `served_model_name` (required), `keep_alive`, `num_parallel` (mapped to `OLLAMA_*` env; OpenAI-compatible readiness) |
| `sglang` | planned | — |

Unknown parameters, unknown engine kinds, and devices absent from local
inventory are rejected before launch.

## Adding a driver

1. Implement the three-method contract with an explicit parameter allow-list.
2. Register it in the `_DRIVERS` map in `drivers.py`.
3. Add unit tests for launch-request construction and parameter rejection
   (see `tests/test_engine_units.py`).

A driver must declare its supported engine versions, required artifact formats,
GPU capabilities, readiness endpoint, graceful-shutdown behavior, and whether it
supports hot reload or replacement only.
