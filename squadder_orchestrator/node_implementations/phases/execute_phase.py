"""Execute phase wrapper."""

from __future__ import annotations

import logging

from squadder_orchestrator.graph_compiler.state_schema import (
    PhaseId,
    PipelineState,
    PipelineStatus,
    StructuredOutput,
    SubtaskState,
    SubtaskStatus,
)
from squadder_orchestrator.graph_compiler.yaml_manifest_parser import PhaseRuntimeConfig
from squadder_orchestrator.integrations.observability import ensure_trace_id
from squadder_orchestrator.node_implementations.status_aggregation import get_ready_subtasks, has_incomplete_subtasks
from squadder_orchestrator.node_implementations.task_unit import TaskUnitRunner


logger = logging.getLogger(__name__)


def _clone_plan(plan: list[SubtaskState]) -> list[SubtaskState]:
    return [
        SubtaskState(
            id=subtask.id,
            role=subtask.role,
            description=subtask.description,
            dependencies=list(subtask.dependencies),
            status=subtask.status,
            retry_count=subtask.retry_count,
            max_retries=subtask.max_retries,
            structured_output=subtask.structured_output,
            reviewer_feedback=subtask.reviewer_feedback,
            tester_result=subtask.tester_result,
            escalation_reason=subtask.escalation_reason,
        )
        for subtask in plan
    ]


def _append_structured_output(
    outputs: list[StructuredOutput],
    structured_output: StructuredOutput,
) -> list[StructuredOutput]:
    existing_ids = {output.subtask_id for output in outputs}
    if structured_output.subtask_id in existing_ids:
        return outputs
    return outputs + [structured_output]


# SEM_BEGIN orchestrator_v1.execute_phase.run_execute_phase:v1
# type: METHOD
# use_case: Выполняет execute-фазу последовательно по ready subtasks и обновляет mutable plan.
# feature:
#   - V1 execute strategy planner_driven/max_concurrent=1 избегает race conditions
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   - phase_config.default_worker_pipeline определён
# post:
#   - возвращает partial PipelineState с обновлёнными subtask statuses и structured_outputs
# invariant:
#   - порядок уже завершённых structured_outputs сохраняется
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: subtask task unit execution failed
# depends:
#   - TaskUnitRunner
#   - get_ready_subtasks
#   - has_incomplete_subtasks
# sft: run execute phase sequentially over ready subtasks and mutate the current plan based on task unit results
# idempotent: false
# logs: query: ExecutePhase trace_id
def run_execute_phase(
    state: PipelineState,
    *,
    task_unit_runner: TaskUnitRunner,
    phase_config: PhaseRuntimeConfig,
) -> PipelineState:
    trace_id = ensure_trace_id(state.get("trace_id"))
    phase_attempts = dict(state.get("phase_attempts", {}))
    phase_attempts["execute"] = phase_attempts.get("execute", 0) + 1
    plan = _clone_plan(list(state.get("plan", [])))
    outputs = list(state.get("structured_outputs", []))
    execution_errors = list(state.get("execution_errors", []))

    logger.info(
        "[ExecutePhase][run_execute_phase][ContextAnchor] trace_id=%s | "
        "Running execute phase. attempt=%d, plan_items=%d",
        trace_id,
        phase_attempts["execute"],
        len(plan),
    )

    while True:
        ready_subtasks = get_ready_subtasks(plan)
        if not ready_subtasks:
            if has_incomplete_subtasks(plan):
                execution_errors.append("No ready subtasks remain; planner must repair the DAG")
                logger.info(
                    "[ExecutePhase][run_execute_phase][DecisionPoint] trace_id=%s | "
                    "Branch: needs_replan. Reason: no_ready_subtasks=True, incomplete_plan=True",
                    trace_id,
                )
                return {
                    "current_phase": PhaseId.EXECUTE,
                    "current_status": PipelineStatus.NEEDS_REPLAN,
                    "phase_attempts": phase_attempts,
                    "plan": plan,
                    "structured_outputs": outputs,
                    "execution_errors": execution_errors,
                    "active_subtask_id": None,
                }
            logger.info(
                "[ExecutePhase][run_execute_phase][StepComplete] trace_id=%s | "
                "Execute phase finished. status=%s, outputs=%d",
                trace_id,
                PipelineStatus.PASS,
                len(outputs),
            )
            return {
                "current_phase": PhaseId.EXECUTE,
                "current_status": PipelineStatus.PASS,
                "phase_attempts": phase_attempts,
                "plan": plan,
                "structured_outputs": outputs,
                "execution_errors": execution_errors,
                "active_subtask_id": None,
            }

        subtask = ready_subtasks[0]
        subtask.status = SubtaskStatus.IN_PROGRESS
        result = task_unit_runner.run(
            phase_id=PhaseId.EXECUTE,
            role_dir=subtask.role,
            pipeline=phase_config.default_worker_pipeline,
            task_context={
                "task_id": state.get("task_id"),
                "user_request": state.get("user_request"),
                "current_state": state.get("current_state", {}),
                "subtask_id": subtask.id,
                "subtask_description": subtask.description,
                "dependencies": subtask.dependencies,
                "checklist_ok": True,
            },
            workspace_root=state["workspace_root"],
            metadata={
                "task_id": state.get("task_id"),
                "phase": PhaseId.EXECUTE,
                "subtask_id": subtask.id,
                "role": subtask.role,
            },
            trace_id=trace_id,
        )

        subtask.retry_count += 1
        if result.status == PipelineStatus.PASS and result.structured_output is not None:
            subtask.status = SubtaskStatus.DONE
            subtask.structured_output = result.structured_output
            subtask.reviewer_feedback = result.review_feedback
            subtask.tester_result = result.test_summary
            outputs = _append_structured_output(outputs, result.structured_output)
            continue

        if result.status in {
            PipelineStatus.ASK_HUMAN,
            PipelineStatus.ESCALATE_TO_HUMAN,
            PipelineStatus.BLOCKED,
        }:
            subtask.status = (
                SubtaskStatus.ESCALATED
                if result.status == PipelineStatus.ESCALATE_TO_HUMAN
                else SubtaskStatus.BLOCKED
            )
            subtask.escalation_reason = "; ".join(result.warnings) or "Human input required"
            logger.info(
                "[ExecutePhase][run_execute_phase][DecisionPoint] trace_id=%s | "
                "Branch: human_gate. Reason: subtask_id=%s, status=%s",
                trace_id,
                subtask.id,
                result.status,
            )
            return {
                "current_phase": PhaseId.EXECUTE,
                "current_status": PipelineStatus.ESCALATE_TO_HUMAN,
                "phase_attempts": phase_attempts,
                "plan": plan,
                "structured_outputs": outputs,
                "execution_errors": execution_errors,
                "active_subtask_id": subtask.id,
                "pending_human_input": result.human_question
                or {
                    "source_phase": PhaseId.EXECUTE,
                    "subtask_id": subtask.id,
                    "question": subtask.escalation_reason,
                },
            }

        subtask.status = (
            SubtaskStatus.FAILED
            if subtask.retry_count >= subtask.max_retries
            else SubtaskStatus.BLOCKED
        )
        subtask.reviewer_feedback = result.review_feedback
        subtask.tester_result = result.test_summary
        subtask.escalation_reason = "; ".join(result.warnings) or result.status
        execution_errors.append(
            f"Subtask {subtask.id} returned {result.status}: {subtask.escalation_reason}"
        )
        logger.info(
            "[ExecutePhase][run_execute_phase][DecisionPoint] trace_id=%s | "
            "Branch: needs_replan. Reason: subtask_id=%s, status=%s, retry_count=%d/%d",
            trace_id,
            subtask.id,
            result.status,
            subtask.retry_count,
            subtask.max_retries,
        )
        return {
            "current_phase": PhaseId.EXECUTE,
            "current_status": PipelineStatus.NEEDS_REPLAN,
            "phase_attempts": phase_attempts,
            "plan": plan,
            "structured_outputs": outputs,
            "execution_errors": execution_errors,
            "active_subtask_id": subtask.id,
        }


# SEM_END orchestrator_v1.execute_phase.run_execute_phase:v1
