"""Driver abstractions for phase runtime execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from workflow_runtime.graph_compiler.state_schema import ExecutionBackend, PhaseId, PipelineStatus, SubRole


# SEM_BEGIN orchestrator_v1.base_driver.driver_request:v1
# type: CLASS
# use_case: Immutable input contract for one runtime driver step.
# feature:
#   - The universal TaskUnit passes the same request shape to mock and OpenHands drivers
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - prompt contains the final composed instruction bundle for one step
#   - metadata may extend context without mutating core request fields
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PhaseId
#   - SubRole
# sft: define immutable driver request contract for one task unit step
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class DriverRequest:
    phase_id: PhaseId
    role_dir: str
    sub_role: SubRole
    execution_backend: ExecutionBackend
    execution_strategy: str | None
    model: str
    prompt: str
    task_context: dict[str, Any]
    working_dir: str
    metadata: dict[str, Any] = field(default_factory=dict)
    system_prompt: str | None = None


# SEM_END orchestrator_v1.base_driver.driver_request:v1


# SEM_BEGIN orchestrator_v1.base_driver.driver_result:v1
# type: CLASS
# use_case: Normalized output contract returned by any runtime driver implementation.
# feature:
#   - TaskUnit must consume a stable status/payload envelope regardless of the selected backend
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - status is always a PipelineStatus value
#   - payload stays JSON/YAML-serializable for downstream processing
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PipelineStatus
# sft: define normalized driver result contract with status payload raw text and conversation id
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class DriverResult:
    status: PipelineStatus
    payload: dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""
    conversation_id: str | None = None
    request_artifact: dict[str, Any] = field(default_factory=dict)
    artifact_refs: dict[str, Any] = field(default_factory=dict)


# SEM_END orchestrator_v1.base_driver.driver_result:v1


# SEM_BEGIN orchestrator_v1.base_driver.base_driver:v1
# type: CLASS
# use_case: Abstract runtime-driver boundary for TaskUnit execution.
# feature:
#   - The universal TaskUnit must stay decoupled from a concrete execution backend
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - subclasses implement the same `run_task()` contract
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define abstract driver interface for runtime backends used by the universal task unit
# idempotent: -
# logs: -
class BaseDriver(ABC):
    # SEM_BEGIN orchestrator_v1.base_driver.base_driver.run_task:v1
    # type: METHOD
    # use_case: Abstract contract for executing one runtime step and returning a normalized result.
    # feature:
    #   - The universal TaskUnit must not know which specific runtime executed the step
    #   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
    # pre:
    #   - request.prompt is not empty
    # post:
    #   - returns a DriverResult with status and payload
    # invariant:
    #   - driver implementation must not mutate the request
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.driver_runtime
    # errors:
    #   - RuntimeError: runtime execution failed
    # depends:
    #   -
    # sft: execute one task unit step through an abstract runtime driver and return normalized result
    # idempotent: false
    # logs: path: orchestrator runtime logs
    @abstractmethod
    def run_task(self, request: DriverRequest) -> DriverResult:
        raise NotImplementedError

    # SEM_END orchestrator_v1.base_driver.base_driver.run_task:v1


# SEM_END orchestrator_v1.base_driver.base_driver:v1
