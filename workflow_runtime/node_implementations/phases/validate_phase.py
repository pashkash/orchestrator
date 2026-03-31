"""Validate phase wrapper."""

from __future__ import annotations

import logging

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineState, PipelineStatus
from workflow_runtime.graph_compiler.yaml_manifest_parser import PhaseRuntimeConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.node_implementations.status_aggregation import merge_structured_outputs
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner


logger = logging.getLogger(__name__)


# SEM_BEGIN orchestrator_v1.validate_phase.run_validate_phase:v1
# type: METHOD
# use_case: Runs the validate phase over merged structured outputs and produces the final_result.
# feature:
#   - Cross-cutting validation in V1 runs after execute and may return the task to replan/human gate
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D4
# pre:
#   - phase_config.pipeline is defined
# post:
#   - returns a partial PipelineState with merged_summary and the validate phase current_status
# invariant:
#   - structured_outputs from state are not mutated
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: validate task unit execution failed
# depends:
#   - merge_structured_outputs
#   - TaskUnitRunner
# sft: run cross-cutting validation for the current structured outputs and set final_result on success
# idempotent: false
# logs: query: ValidatePhase trace_id
def run_validate_phase(
    state: PipelineState,
    *,
    task_unit_runner: TaskUnitRunner,
    phase_config: PhaseRuntimeConfig,
) -> PipelineState:
    trace_id = ensure_trace_id(state.get("trace_id"))
    phase_attempts = dict(state.get("phase_attempts", {}))
    phase_attempts["validate"] = phase_attempts.get("validate", 0) + 1
    merged_summary = merge_structured_outputs(list(state.get("structured_outputs", [])), trace_id=trace_id)

    logger.info(
        "[ValidatePhase][run_validate_phase][ContextAnchor] trace_id=%s | "
        "Running validate phase. attempt=%d",
        trace_id,
        phase_attempts["validate"],
    )

    result = task_unit_runner.run(
        phase_id=PhaseId.VALIDATE,
        role_dir=phase_config.role_dir or "supervisor",
        pipeline=phase_config.pipeline,
        task_context={
            "task_id": state.get("task_id"),
            "user_request": state.get("user_request"),
            "current_state": state.get("current_state", {}),
            "merged_summary": merged_summary,
            "structured_outputs": [output.subtask_id for output in state.get("structured_outputs", [])],
        },
        workspace_root=state["workspace_root"],
        metadata={"task_id": state.get("task_id"), "phase": PhaseId.VALIDATE},
        trace_id=trace_id,
    )

    updates: PipelineState = {
        "current_phase": PhaseId.VALIDATE,
        "current_status": result.status,
        "phase_attempts": phase_attempts,
        "merged_summary": merged_summary,
        "phase_outputs": {
            **state.get("phase_outputs", {}),
            PhaseId.VALIDATE: result.payload,
        },
    }
    if result.status == PipelineStatus.PASS:
        updates["final_result"] = str(
            result.payload.get("final_result") or "Validation succeeded with no conflicts"
        )
    if result.human_question:
        updates["pending_human_input"] = result.human_question
    logger.info(
        "[ValidatePhase][run_validate_phase][StepComplete] trace_id=%s | "
        "Validate phase finished. status=%s",
        trace_id,
        result.status,
    )
    return updates


# SEM_END orchestrator_v1.validate_phase.run_validate_phase:v1
