"""Helpers for mutable plan traversal and structured output merging."""

from __future__ import annotations

import logging
from typing import Any

from squadder_orchestrator.graph_compiler.state_schema import StructuredOutput, SubtaskState
from squadder_orchestrator.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)


def get_ready_subtasks(plan: list[SubtaskState]) -> list[SubtaskState]:
    completed = {subtask.id for subtask in plan if subtask.status == "done"}
    ready: list[SubtaskState] = []
    for subtask in plan:
        if subtask.status != "pending":
            continue
        if all(dependency in completed for dependency in subtask.dependencies):
            ready.append(subtask)
    return ready


def has_incomplete_subtasks(plan: list[SubtaskState]) -> bool:
    return any(subtask.status in {"pending", "in_progress"} for subtask in plan)


# SEM_BEGIN orchestrator_v1.status_aggregation.merge_structured_outputs:v1
# type: METHOD
# use_case: Merge-ит StructuredOutput-ы в aggregated summary для validate phase.
# feature:
#   - validate phase должен находить file conflicts и собирать единый change summary
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4/D6
# pre:
#   - outputs содержит StructuredOutput entries из execute phase
# post:
#   - возвращает summary с conflicts, changed_files, warnings и commits
# invariant:
#   - outputs не мутируется
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
