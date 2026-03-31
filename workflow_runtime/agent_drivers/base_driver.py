"""Driver abstractions for phase runtime execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineStatus, SubRole


@dataclass(frozen=True, slots=True)
class DriverRequest:
    phase_id: PhaseId
    role_dir: str
    sub_role: SubRole
    model: str
    prompt: str
    task_context: dict[str, Any]
    workspace_root: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DriverResult:
    status: PipelineStatus
    payload: dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""
    conversation_id: str | None = None


# SEM_BEGIN orchestrator_v1.base_driver.run_task:v1
# type: METHOD
# use_case: Contract for any runtime driver (mock or OpenHands).
# feature:
#   - The universal TaskUnit must not know which specific runtime executed the step
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4/D5
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
class BaseDriver(ABC):
    @abstractmethod
    def run_task(self, request: DriverRequest) -> DriverResult:
        raise NotImplementedError


# SEM_END orchestrator_v1.base_driver.run_task:v1
