"""Collect phase wrapper."""

from __future__ import annotations

from lmnr import observe

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineState, PipelineStatus
from workflow_runtime.graph_compiler.yaml_manifest_parser import PhaseRuntimeConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.collect_phase.run_collect_phase:v1
# type: METHOD
# use_case: Runs the collect phase and updates the current environment state snapshot.
# feature:
#   - The V1 phase graph starts with collect and uses a single TaskUnit
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D4
# pre:
#   - phase_config.pipeline is defined
# post:
#   - returns a partial PipelineState with current_phase=collect and the new current_status
# invariant:
#   - the original state is not mutated in-place
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: collect task unit execution failed
# depends:
#   - TaskUnitRunner
# sft: run the collect phase and persist the current environment snapshot in pipeline state
# idempotent: false
# logs: query: CollectPhase trace_id
@observe(name="phase_collect")
def run_collect_phase(
    state: PipelineState,
    *,
    task_unit_runner: TaskUnitRunner,
    phase_config: PhaseRuntimeConfig,
) -> PipelineState:
    trace_id = ensure_trace_id(state.get("trace_id"))
    phase_attempts = dict(state.get("phase_attempts", {}))
    phase_attempts["collect"] = phase_attempts.get("collect", 0) + 1

    logger.info(
        "[CollectPhase][run_collect_phase][ContextAnchor] trace_id=%s | "
        "Running collect phase. attempt=%d",
        trace_id,
        phase_attempts["collect"],
    )

    result = task_unit_runner.run(
        phase_id=PhaseId.COLLECT,
        role_dir=phase_config.role_dir or "collector",
        pipeline=phase_config.pipeline,
        task_context={
            "task_id": state.get("task_id"),
            "user_request": state.get("user_request"),
            "current_state": state.get("current_state", {}),
            "source_workspace_root": state.get("workspace_root", ""),
            "task_worktree_root": state.get("task_worktree_root", ""),
            "task_dir_path": state.get("task_dir_path", ""),
            "task_card_path": state.get("task_card_path", ""),
            "openhands_conversations_dir": state.get("openhands_conversations_dir", ""),
            "methodology_root_runtime": state.get("methodology_root_runtime", ""),
            "methodology_agents_entrypoint": state.get("methodology_agents_entrypoint", ""),
        },
        working_dir=state["task_worktree_root"],
        metadata={"task_id": state.get("task_id"), "phase": PhaseId.COLLECT},
        trace_id=trace_id,
    )

    updates: PipelineState = {
        "current_phase": PhaseId.COLLECT,
        "current_status": result.status,
        "phase_attempts": phase_attempts,
        "phase_outputs": {
            **state.get("phase_outputs", {}),
            PhaseId.COLLECT: result.payload,
        },
    }
    if result.status == PipelineStatus.PASS:
        updates["current_state"] = dict(result.payload.get("current_state", {}))
    logger.info(
        "[CollectPhase][run_collect_phase][StepComplete] trace_id=%s | "
        "Collect phase finished. status=%s",
        trace_id,
        result.status,
    )
    return updates


# SEM_END orchestrator_v1.collect_phase.run_collect_phase:v1
