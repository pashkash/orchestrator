"""Collect phase wrapper."""

from __future__ import annotations

import logging

from squadder_orchestrator.graph_compiler.state_schema import PhaseId, PipelineState, PipelineStatus
from squadder_orchestrator.graph_compiler.yaml_manifest_parser import PhaseRuntimeConfig
from squadder_orchestrator.integrations.observability import ensure_trace_id
from squadder_orchestrator.node_implementations.task_unit import TaskUnitRunner


logger = logging.getLogger(__name__)


# SEM_BEGIN orchestrator_v1.collect_phase.run_collect_phase:v1
# type: METHOD
# use_case: Выполняет collect-фазу и обновляет snapshot текущего состояния окружения.
# feature:
#   - V1 phase graph начинается с collect и использует единый TaskUnit
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D4
# pre:
#   - phase_config.pipeline определён
# post:
#   - возвращает partial PipelineState с current_phase=collect и новым current_status
# invariant:
#   - исходный state не мутируется in-place
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
        },
        workspace_root=state["workspace_root"],
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
