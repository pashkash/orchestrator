"""Plan phase wrapper."""

from __future__ import annotations

import logging

from squadder_orchestrator.graph_compiler.state_schema import (
    PhaseId,
    PipelineState,
    PipelineStatus,
    SubtaskState,
    SubtaskStatus,
)
from squadder_orchestrator.graph_compiler.yaml_manifest_parser import PhaseRuntimeConfig
from squadder_orchestrator.integrations.observability import ensure_trace_id
from squadder_orchestrator.node_implementations.task_unit import TaskUnitRunner


logger = logging.getLogger(__name__)


def _merge_plan(existing_plan: list[SubtaskState], planned_items: list[dict]) -> list[SubtaskState]:
    existing_by_id = {subtask.id: subtask for subtask in existing_plan}
    merged: list[SubtaskState] = []
    planned_ids: set[str] = set()

    for item in planned_items:
        subtask_id = str(item["id"])
        planned_ids.add(subtask_id)
        existing = existing_by_id.get(subtask_id)
        if existing:
            existing.role = str(item["role"])
            existing.description = str(item["description"])
            existing.dependencies = list(item.get("dependencies", []))
            existing.max_retries = int(item.get("max_retries", existing.max_retries))
            merged.append(existing)
            continue
        merged.append(
            SubtaskState(
                id=subtask_id,
                role=str(item["role"]),
                description=str(item["description"]),
                dependencies=list(item.get("dependencies", [])),
                max_retries=int(item.get("max_retries", 3)),
            )
        )

    for existing in existing_plan:
        if existing.id not in planned_ids and existing.status == SubtaskStatus.DONE:
            merged.append(existing)
    return merged


# SEM_BEGIN orchestrator_v1.plan_phase.run_plan_phase:v1
# type: METHOD
# use_case: Выполняет plan-фазу и обновляет mutable DAG-план без потери уже завершённых подзадач.
# feature:
#   - План в V1 mutates incrementally instead of full reset after every iteration
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D6
# pre:
#   - phase_config.pipeline определён
# post:
#   - возвращает partial PipelineState с current_phase=plan и обновлённым plan при PASS
# invariant:
#   - завершённые subtasks сохраняются, если planner их больше не возвращает
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: planner task unit execution failed
# depends:
#   - TaskUnitRunner
#   - _merge_plan
# sft: run the planning phase and merge the new DAG plan with existing completed subtasks
# idempotent: false
# logs: query: PlanPhase trace_id
def run_plan_phase(
    state: PipelineState,
    *,
    task_unit_runner: TaskUnitRunner,
    phase_config: PhaseRuntimeConfig,
) -> PipelineState:
    trace_id = ensure_trace_id(state.get("trace_id"))
    phase_attempts = dict(state.get("phase_attempts", {}))
    phase_attempts["plan"] = phase_attempts.get("plan", 0) + 1

    logger.info(
        "[PlanPhase][run_plan_phase][ContextAnchor] trace_id=%s | "
        "Running plan phase. attempt=%d",
        trace_id,
        phase_attempts["plan"],
    )

    existing_plan = list(state.get("plan", []))
    result = task_unit_runner.run(
        phase_id=PhaseId.PLAN,
        role_dir=phase_config.role_dir or "supervisor",
        pipeline=phase_config.pipeline,
        task_context={
            "task_id": state.get("task_id"),
            "user_request": state.get("user_request"),
            "current_state": state.get("current_state", {}),
            "existing_plan": [
                {
                    "id": subtask.id,
                    "role": subtask.role,
                    "description": subtask.description,
                    "dependencies": subtask.dependencies,
                    "status": subtask.status,
                }
                for subtask in existing_plan
            ],
        },
        workspace_root=state["workspace_root"],
        metadata={"task_id": state.get("task_id"), "phase": PhaseId.PLAN},
        trace_id=trace_id,
    )

    updates: PipelineState = {
        "current_phase": PhaseId.PLAN,
        "current_status": result.status,
        "phase_attempts": phase_attempts,
        "phase_outputs": {
            **state.get("phase_outputs", {}),
            PhaseId.PLAN: result.payload,
        },
    }
    if result.status == PipelineStatus.PASS:
        updates["plan"] = _merge_plan(existing_plan, list(result.payload.get("plan", [])))
    if result.human_question:
        updates["pending_human_input"] = result.human_question
    logger.info(
        "[PlanPhase][run_plan_phase][StepComplete] trace_id=%s | "
        "Plan phase finished. status=%s, planned_items=%d",
        trace_id,
        result.status,
        len(updates.get("plan", existing_plan)),
    )
    return updates


# SEM_END orchestrator_v1.plan_phase.run_plan_phase:v1
