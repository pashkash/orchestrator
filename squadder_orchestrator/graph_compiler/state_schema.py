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
# use_case: Описывает одно файловое изменение внутри StructuredOutput.
# feature:
#   - Нужен для merge и cross-cutting validation в validate phase
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4/D5/D6
# pre:
#   -
# post:
#   -
# invariant:
#   - все поля обязательны
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
# use_case: Описывает обязательный результат работы executor-а или полного TaskUnit.
# feature:
#   - V1 использует StructuredOutput как общий контракт между execute и validate
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0/D4/D5
# pre:
#   -
# post:
#   -
# invariant:
#   - subtask_id уникален в рамках одного task_id
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
# use_case: Хранит mutable состояние одной подзадачи в плане.
# feature:
#   - Execute phase работает последовательно по списку SubtaskState
#   - mutable plan нужен чтобы supervisor мог чинить, отменять и переиспользовать шаги
# pre:
#   -
# post:
#   -
# invariant:
#   - id уникален внутри одного plan
#   - role не пустой
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
# use_case: Нормализованный результат выполнения universal TaskUnit независимо от фазы.
# feature:
#   - phase wrappers читают один и тот же результат и решают как обновить PipelineState
#   - V1 design dump: Executor -> Reviewer -> Guardrails -> Tester
# pre:
#   -
# post:
#   -
# invariant:
#   - status всегда один из допустимых PipelineStatus
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
# use_case: Полное состояние одного V1 orchestrator run.
# feature:
#   - top-level LangGraph хранит только phase-level control state и mutable plan
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0-D7
# pre:
#   -
# post:
#   -
# invariant:
#   - total=False нужен LangGraph для частичных updates между phase nodes
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
