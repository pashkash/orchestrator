"""Helpers for mutable plan traversal and structured output merging."""

from __future__ import annotations

from typing import Any

from workflow_runtime.graph_compiler.state_schema import StructuredOutput, SubtaskState, SubtaskStatus
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.status_aggregation.get_ready_subtasks:v1
# type: METHOD
# use_case: Selects subtasks whose dependencies are already completed and are ready for execution.
# feature:
#   - Execute phase in V1 is sequential, so readiness must be recomputed from the mutable plan on each loop
# pre:
#   - plan contains SubtaskState items from the current mutable plan
# post:
#   - returns pending subtasks whose dependencies are all done
# invariant:
#   - plan is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - SubtaskStatus
# sft: compute ready subtasks from a mutable plan by checking completed dependencies
# idempotent: true
# logs: -
def get_ready_subtasks(plan: list[SubtaskState]) -> list[SubtaskState]:
    completed = {subtask.id for subtask in plan if subtask.status == SubtaskStatus.DONE}
    ready: list[SubtaskState] = []
    for subtask in plan:
        if subtask.status != SubtaskStatus.PENDING:
            continue
        if all(dependency in completed for dependency in subtask.dependencies):
            ready.append(subtask)
    return ready


# SEM_END orchestrator_v1.status_aggregation.get_ready_subtasks:v1


# SEM_BEGIN orchestrator_v1.status_aggregation.has_incomplete_subtasks:v1
# type: METHOD
# use_case: Checks whether the current plan still contains pending or running work.
# feature:
#   - Execute phase needs to distinguish a completed plan from a deadlocked plan with no ready subtasks
# pre:
#   -
# post:
#   - returns true when the plan still has pending or in-progress subtasks
# invariant:
#   - plan is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - SubtaskStatus
# sft: detect whether a mutable plan still contains incomplete subtasks
# idempotent: true
# logs: -
def has_incomplete_subtasks(plan: list[SubtaskState]) -> bool:
    return any(subtask.status in {SubtaskStatus.PENDING, SubtaskStatus.IN_PROGRESS} for subtask in plan)


# SEM_END orchestrator_v1.status_aggregation.has_incomplete_subtasks:v1


# SEM_BEGIN orchestrator_v1.status_aggregation.merge_structured_outputs:v1
# type: METHOD
# use_case: Merges StructuredOutputs into an aggregated summary for the validate phase.
# feature:
#   - The validate phase must detect file conflicts and collect a unified change summary
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4/D6
# pre:
#   - outputs contains StructuredOutput entries from the execute phase
# post:
#   - returns a summary with conflicts, changed_files, warnings, and commits
# invariant:
#   - outputs is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - StructuredOutput
# sft: aggregate structured outputs and detect file conflicts before validate phase
# idempotent: true
# logs: query: duplicate file changes across structured outputs
def merge_structured_outputs(
    outputs: list[StructuredOutput],
    *,
    trace_id: str | None = None,
) -> dict[str, Any]:
    resolved_trace_id = ensure_trace_id(trace_id)
    file_owners: dict[str, list[str]] = {}
    warnings: list[str] = []
    commits: list[dict[str, Any]] = []

    logger.info(
        "[StatusAggregation][merge_structured_outputs][ContextAnchor] trace_id=%s | "
        "Merging structured outputs. count=%d",
        resolved_trace_id,
        len(outputs),
    )

    for output in outputs:
        commits.extend(output.commits)
        warnings.extend(output.warnings)
        for change in output.changes:
            file_owners.setdefault(change.file, []).append(output.subtask_id)

    conflicts = [
        f"{file_path}: {', '.join(sorted(set(subtasks)))}"
        for file_path, subtasks in sorted(file_owners.items())
        if len(set(subtasks)) > 1
    ]

    summary = {
        "changed_files": sorted(file_owners.keys()),
        "conflicts": conflicts,
        "warnings": warnings,
        "commits": commits,
        "subtasks_completed": [output.subtask_id for output in outputs],
    }
    logger.info(
        "[StatusAggregation][merge_structured_outputs][StepComplete] trace_id=%s | "
        "Merge complete. changed_files=%d, conflicts=%d",
        resolved_trace_id,
        len(summary["changed_files"]),
        len(conflicts),
    )
    return summary


# SEM_END orchestrator_v1.status_aggregation.merge_structured_outputs:v1
