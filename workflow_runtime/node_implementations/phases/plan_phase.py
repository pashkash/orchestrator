"""Plan phase wrapper."""

from __future__ import annotations

from lmnr import observe

from workflow_runtime.graph_compiler.state_schema import (
    PhaseId,
    PipelineState,
    PipelineStatus,
    SubtaskState,
    SubtaskStatus,
)
from workflow_runtime.graph_compiler.yaml_manifest_parser import PhaseRuntimeConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.integrations.tasks_storage import build_task_artifact_context, sync_plan_to_task_artifacts
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.plan_phase._merge_plan:v1
# type: METHOD
# use_case: Merges the new plan with the existing one, preserving already completed subtasks.
# feature:
#   - The mutable plan in V1 allows the supervisor to update the plan without losing progress
# pre:
#   - planned_items contains dicts with a required "id" key
# post:
#   - returns a merged list where done subtasks are not lost
# invariant:
#   - the order of planned_items is preserved
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - KeyError: pre[0] violated — missing "id" key in a plan item
# depends:
#   - SubtaskState
# sft: merge new planner output into existing subtask list preserving completed items
# idempotent: true
# logs: -
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
            if existing.status != SubtaskStatus.DONE:
                existing.status = SubtaskStatus.PENDING
                existing.structured_output = None
                existing.reviewer_feedback = None
                existing.tester_result = None
                existing.escalation_reason = None
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


# SEM_END orchestrator_v1.plan_phase._merge_plan:v1


# SEM_BEGIN orchestrator_v1.plan_phase.run_plan_phase:v1
# type: METHOD
# use_case: Runs the plan phase and updates the mutable DAG plan without losing already completed subtasks.
# feature:
#   - The plan in V1 mutates incrementally instead of full reset after every iteration
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D6
# pre:
#   - phase_config.pipeline is defined
# post:
#   - returns a partial PipelineState with current_phase=plan and an updated plan on PASS
# invariant:
#   - completed subtasks are preserved if the planner no longer returns them
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
@observe(name="phase_plan")
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
    task_artifact_context = build_task_artifact_context(
        state.get("task_id"),
        task_dir_path=state.get("task_dir_path"),
        task_card_path=state.get("task_card_path"),
        openhands_conversations_dir=state.get("openhands_conversations_dir"),
    )
    result = task_unit_runner.run(
        phase_id=PhaseId.PLAN,
        role_dir=phase_config.role_dir or "supervisor",
        pipeline=phase_config.pipeline,
        task_context={
            "task_id": state.get("task_id"),
            "user_request": state.get("user_request"),
            "current_state": state.get("current_state", {}),
            "source_workspace_root": state.get("workspace_root", ""),
            "task_worktree_root": state.get("task_worktree_root", ""),
            "methodology_root_runtime": state.get("methodology_root_runtime", ""),
            "methodology_agents_entrypoint": state.get("methodology_agents_entrypoint", ""),
            **task_artifact_context,
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
        working_dir=state["task_worktree_root"],
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
        sync_plan_to_task_artifacts(
            task_context={
                "task_id": state.get("task_id"),
                "user_request": state.get("user_request"),
                "source_workspace_root": state.get("workspace_root", ""),
                "task_worktree_root": state.get("task_worktree_root", ""),
                **task_artifact_context,
            },
            plan=list(updates["plan"]),
        )
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
