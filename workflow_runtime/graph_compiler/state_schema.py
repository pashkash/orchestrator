"""V1 state schema for the Squadder orchestrator.

The phase graph is intentionally small and stable:
collect -> plan -> execute -> validate -> human_gate.

All dynamic work happens inside a universal TaskUnit and inside the mutable
plan stored in PipelineState.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypedDict


class PhaseId(StrEnum):
    COLLECT = "collect"
    PLAN = "plan"
    EXECUTE = "execute"
    VALIDATE = "validate"
    HUMAN_GATE = "human_gate"


class PipelineStatus(StrEnum):
    PASS = "PASS"
    NEEDS_INFO = "NEEDS_INFO"
    NEEDS_MORE_SNAPSHOT = "NEEDS_MORE_SNAPSHOT"
    NEEDS_REPLAN = "NEEDS_REPLAN"
    NEEDS_FIX_EXECUTOR = "NEEDS_FIX_EXECUTOR"
    NEEDS_FIX_REVIEW = "NEEDS_FIX_REVIEW"
    NEEDS_FIX_TESTS = "NEEDS_FIX_TESTS"
    ASK_HUMAN = "ASK_HUMAN"
    ESCALATE_TO_HUMAN = "ESCALATE_TO_HUMAN"
    BLOCKED = "BLOCKED"


class SubtaskStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    ESCALATED = "escalated"


class StructuredOutputStatus(StrEnum):
    DONE = "done"
    FAILED = "failed"
    ESCALATED = "escalated"
    CANCELLED = "cancelled"


class DriverMode(StrEnum):
    MOCK = "mock"
    OPENHANDS = "openhands"


class SubRole(StrEnum):
    EXECUTOR = "executor"
    REVIEWER = "reviewer"
    TESTER = "tester"


# SEM_BEGIN orchestrator_v1.state.file_change:v1
# type: CLASS
# use_case: Describes a single file change inside StructuredOutput.
# feature:
#   - Required for merge and cross-cutting validation in the validate phase
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4/D5/D6
# pre:
#   -
# post:
#   -
# invariant:
#   - all fields are required
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define immutable file change record for orchestrator structured output
# idempotent: -
# logs: -
@dataclass(slots=True)
class FileChange:
    file: str
    type: Literal["created", "modified", "deleted"]
    description: str


# SEM_END orchestrator_v1.state.file_change:v1


# SEM_BEGIN orchestrator_v1.state.structured_output:v1
# type: CLASS
# use_case: Describes the required result of an executor or a complete TaskUnit run.
# feature:
#   - V1 uses StructuredOutput as the shared contract between execute and validate
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0/D4/D5
# pre:
#   -
# post:
#   -
# invariant:
#   - subtask_id is unique within a single task_id
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - FileChange
# sft: define structured output contract for a completed subtask in the orchestrator
# idempotent: -
# logs: -
@dataclass(slots=True)
class StructuredOutput:
    task_id: str
    subtask_id: str
    role: str
    status: StructuredOutputStatus
    changes: list[FileChange] = field(default_factory=list)
    commands_executed: list[str] = field(default_factory=list)
    tests_passed: list[str] = field(default_factory=list)
    commits: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    escalation: dict[str, Any] | None = None
    summary: str = ""


# SEM_END orchestrator_v1.state.structured_output:v1


# SEM_BEGIN orchestrator_v1.state.subtask_state:v1
# type: CLASS
# use_case: Holds the mutable state of one subtask in the plan.
# feature:
#   - Execute phase runs sequentially over the SubtaskState list
#   - mutable plan is needed so a supervisor can fix, cancel, and reuse steps
# pre:
#   -
# post:
#   -
# invariant:
#   - id is unique within a single plan
#   - role is non-empty
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - StructuredOutput
# sft: define mutable subtask state for sequential planner-driven execution
# idempotent: -
# logs: -
@dataclass(slots=True)
class SubtaskState:
    id: str
    role: str
    description: str
    dependencies: list[str] = field(default_factory=list)
    status: SubtaskStatus = SubtaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    structured_output: StructuredOutput | None = None
    reviewer_feedback: str | None = None
    tester_result: str | None = None
    escalation_reason: str | None = None


# SEM_END orchestrator_v1.state.subtask_state:v1


# SEM_BEGIN orchestrator_v1.state.task_unit_result:v1
# type: CLASS
# use_case: Normalized result of a universal TaskUnit execution regardless of phase.
# feature:
#   - phase wrappers read the same result and decide how to update PipelineState
#   - V1 design dump: Executor -> Reviewer -> Guardrails -> Tester
# pre:
#   -
# post:
#   -
# invariant:
#   - status is always one of the allowed PipelineStatus values
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - StructuredOutput
# sft: define normalized task unit result carrying phase status payload and optional structured output
# idempotent: -
# logs: -
@dataclass(slots=True)
class TaskUnitResult:
    status: PipelineStatus
    payload: dict[str, Any] = field(default_factory=dict)
    structured_output: StructuredOutput | None = None
    review_feedback: str | None = None
    test_summary: str | None = None
    warnings: list[str] = field(default_factory=list)
    human_question: dict[str, Any] | None = None
    raw_text: str = ""
    conversation_id: str | None = None


# SEM_END orchestrator_v1.state.task_unit_result:v1


# SEM_BEGIN orchestrator_v1.state.pipeline_state:v1
# type: CLASS
# use_case: Full state of a single V1 orchestrator run.
# feature:
#   - top-level LangGraph stores only phase-level control state and mutable plan
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0-D7
# pre:
#   -
# post:
#   -
# invariant:
#   - total=False is required by LangGraph for partial updates between phase nodes
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - SubtaskState
#   - StructuredOutput
# sft: define pipeline state for a phase-driven orchestrator with mutable plan and human gate
# idempotent: -
# logs: -
class PipelineState(TypedDict, total=False):
    task_id: str
    user_request: str
    trace_id: str
    workspace_root: str

    current_phase: PhaseId
    current_status: PipelineStatus
    phase_attempts: dict[str, int]

    current_state: dict[str, Any]
    plan: list[SubtaskState]
    active_subtask_id: str | None
    structured_outputs: list[StructuredOutput]
    merged_summary: dict[str, Any]

    phase_outputs: dict[str, dict[str, Any]]
    execution_errors: list[str]
    human_decisions: list[dict[str, Any]]
    pending_human_input: dict[str, Any] | None

    final_result: str | None
    commits: list[str]


# SEM_END orchestrator_v1.state.pipeline_state:v1
