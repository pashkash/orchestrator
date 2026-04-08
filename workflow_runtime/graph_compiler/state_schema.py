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


# SEM_BEGIN orchestrator_v1.state.phase_id:v1
# type: CLASS
# use_case: Enumerates the fixed top-level phases of the V1 orchestrator graph.
# feature:
#   - The runtime graph intentionally stays small and phase-driven in V1
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0-D3
# pre:
#   -
# post:
#   -
# invariant:
#   - enum values match flow.yaml phase ids
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define enum of top-level V1 orchestration phases aligned with flow manifest ids
# idempotent: -
# logs: -
class PhaseId(StrEnum):
    COLLECT = "collect"
    PLAN = "plan"
    EXECUTE = "execute"
    VALIDATE = "validate"
    HUMAN_GATE = "human_gate"


# SEM_END orchestrator_v1.state.phase_id:v1


# SEM_BEGIN orchestrator_v1.state.pipeline_status:v1
# type: CLASS
# use_case: Enumerates normalized phase and task-unit statuses used across the runtime.
# feature:
#   - Phase routing and task-unit repair loops depend on one shared status vocabulary
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - enum values match flow.yaml transition statuses and runtime contracts
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define normalized pipeline status enum shared by task unit outputs and phase routing
# idempotent: -
# logs: -
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


# SEM_END orchestrator_v1.state.pipeline_status:v1


# SEM_BEGIN orchestrator_v1.state.subtask_status:v1
# type: CLASS
# use_case: Enumerates lifecycle states for mutable-plan subtasks.
# feature:
#   - Execute phase mutates the plan incrementally and tracks retries escalations and cancellation per subtask
#   - Task card 2026-03-24_1800__multi-agent-system-design, D5-D6
# pre:
#   -
# post:
#   -
# invariant:
#   - enum values remain lowercase because they are embedded into plan payloads and task artifacts
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define mutable plan subtask lifecycle statuses for sequential V1 execution
# idempotent: -
# logs: -
class SubtaskStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    ESCALATED = "escalated"


# SEM_END orchestrator_v1.state.subtask_status:v1


# SEM_BEGIN orchestrator_v1.state.structured_output_status:v1
# type: CLASS
# use_case: Enumerates completion statuses stored inside StructuredOutput records.
# feature:
#   - StructuredOutput needs a portable status field independent of richer pipeline routing statuses
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D6
# pre:
#   -
# post:
#   -
# invariant:
#   - values remain lowercase for artifact serialization compatibility
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define portable structured output status enum for executor result artifacts
# idempotent: -
# logs: -
class StructuredOutputStatus(StrEnum):
    DONE = "done"
    FAILED = "failed"
    ESCALATED = "escalated"
    CANCELLED = "cancelled"


# SEM_END orchestrator_v1.state.structured_output_status:v1


# SEM_BEGIN orchestrator_v1.state.driver_mode:v1
# type: CLASS
# use_case: Enumerates supported runtime-driver backends for graph compilation.
# feature:
#   - The same graph can switch between mock and live routed runtime without changing phase wrappers
#   - Task card 2026-04-07_1800__orchestrator-latency-and-observability, D13-D15
# pre:
#   -
# post:
#   -
# invariant:
#   - canonical enum values match current runtime semantics, while deprecated aliases are normalized separately
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define runtime driver mode enum for mock and live hybrid execution backends
# idempotent: -
# logs: -
class DriverMode(StrEnum):
    MOCK = "mock"
    LIVE = "live"

    # SEM_BEGIN orchestrator_v1.state.driver_mode.from_raw:v1
    # type: METHOD
    # use_case: Нормализует raw driver mode из env/CLI/tests в canonical DriverMode с поддержкой deprecated alias.
    # feature:
    #   - Migration от misleading `openhands` name к честному `live` не должна ломать существующие env/tests
    #   - Task card 2026-04-07_1800__orchestrator-latency-and-observability, D13
    # pre:
    #   - value is DriverMode or string-like identifier
    # post:
    #   - returns canonical DriverMode value
    # invariant:
    #   - alias `openhands` always resolves to DriverMode.LIVE
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   - ValueError: raw mode is unsupported
    # depends:
    #   -
    # sft: normalize raw runtime driver mode values and deprecated aliases into the canonical enum
    # idempotent: true
    # logs: -
    @classmethod
    def from_raw(cls, value: "DriverMode | str") -> "DriverMode":
        raw_value = value if isinstance(value, cls) else str(value).strip().lower()
        if raw_value == "openhands":
            return cls.LIVE
        return cls(raw_value)
    # SEM_END orchestrator_v1.state.driver_mode.from_raw:v1


# SEM_END orchestrator_v1.state.driver_mode:v1


# SEM_BEGIN orchestrator_v1.state.execution_backend:v1
# type: CLASS
# use_case: Enumerates concrete execution backends available to one task-unit step.
# feature:
#   - Runtime must be able to switch any phase step between OpenHands direct LLM and tool-calling execution from YAML
#   - Task card 2026-04-05_1900__oh-laminar-otel-gui, T43
# pre:
#   -
# post:
#   -
# invariant:
#   - enum values match the runtime YAML backend names
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define execution backend enum for per-step runtime backend selection in the orchestrator
# idempotent: -
# logs: -
class ExecutionBackend(StrEnum):
    OPENHANDS = "openhands"
    DIRECT_LLM = "direct_llm"
    LANGCHAIN_TOOLS = "langchain_tools"


# SEM_END orchestrator_v1.state.execution_backend:v1


# SEM_BEGIN orchestrator_v1.state.sub_role:v1
# type: CLASS
# use_case: Enumerates the internal roles of one universal TaskUnit pipeline step.
# feature:
#   - Prompt composition and guardrails differentiate executor reviewer and tester within the same phase
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4
# pre:
#   -
# post:
#   -
# invariant:
#   - values stay aligned with runtime manifests and shared prompt filenames
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define task unit sub-role enum for executor reviewer and tester prompt flows
# idempotent: -
# logs: -
class SubRole(StrEnum):
    EXECUTOR = "executor"
    REVIEWER = "reviewer"
    TESTER = "tester"


# SEM_END orchestrator_v1.state.sub_role:v1


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


# SEM_BEGIN orchestrator_v1.state.runtime_artifact_ref:v1
# type: CLASS
# use_case: Stores a compact pointer to one durable runtime artifact persisted on disk.
# feature:
#   - PipelineState must index prompt/payload/guardrail/human-gate artifacts without embedding large blobs
#   - Task card 2026-04-08_2107__design-approval-aware-runtime-storage, D3-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - path points to a file inside task-local runtime artifacts storage
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define a compact typed pointer to one persisted runtime artifact file
# idempotent: -
# logs: -
class RuntimeArtifactRef(TypedDict, total=False):
    artifact_kind: str
    path: str
    phase_id: str
    subtask_id: str
    sub_role: str
    attempt: int
    created_at: str
    trace_id: str
    sha256: str


# SEM_END orchestrator_v1.state.runtime_artifact_ref:v1


# SEM_BEGIN orchestrator_v1.state.runtime_step_ref:v1
# type: CLASS
# use_case: Stores a compact summary pointer for one phase/subtask/sub-role attempt.
# feature:
#   - LangGraph state must expose an append-only journal of runtime steps and the latest summary per key
#   - Task card 2026-04-08_2107__design-approval-aware-runtime-storage, D4-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - summary_path points to the canonical summary file for this attempt
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - RuntimeArtifactRef
# sft: define a compact summary record for one persisted runtime step attempt
# idempotent: -
# logs: -
class RuntimeStepRef(TypedDict, total=False):
    step_key: str
    phase_id: str
    subtask_id: str
    sub_role: str
    attempt: int
    status: str
    summary_path: str
    artifact_refs: list[RuntimeArtifactRef]


# SEM_END orchestrator_v1.state.runtime_step_ref:v1


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
    executor_attempts_used: int = 0
    warnings: list[str] = field(default_factory=list)
    human_question: dict[str, Any] | None = None
    raw_text: str = ""
    conversation_id: str | None = None
    runtime_step_refs: list[RuntimeStepRef] = field(default_factory=list)
    latest_step_ref_by_key: dict[str, RuntimeStepRef] = field(default_factory=dict)
    pending_approval_ref: RuntimeArtifactRef | None = None


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
    task_worktree_root: str
    primary_workspace_repo_id: str
    source_workspace_roots: dict[str, str]
    role_workspace_repo_map: dict[str, str]
    task_workspace_repos: dict[str, str]
    methodology_root_host: str
    methodology_root_runtime: str
    methodology_agents_entrypoint: str
    task_dir_path: str
    task_card_path: str
    openhands_conversations_dir: str

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
    runtime_step_refs: list[RuntimeStepRef]
    latest_step_ref_by_key: dict[str, RuntimeStepRef]
    pending_approval_ref: RuntimeArtifactRef | None
    human_decision_refs: list[RuntimeArtifactRef]
    cleanup_manifest_ref: RuntimeArtifactRef | None

    final_result: str | None
    commits: list[str]


# SEM_END orchestrator_v1.state.pipeline_state:v1
