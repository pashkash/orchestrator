"""Transition evaluators for the V1 phase graph."""

from __future__ import annotations

import logging

from squadder_orchestrator.graph_compiler.state_schema import PipelineState
from squadder_orchestrator.graph_compiler.yaml_manifest_parser import FlowManifest
from squadder_orchestrator.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)


def collect_phase_targets(manifest: FlowManifest, phase_id: str) -> list[str]:
    targets = {
        transition.to_phase
        for transition in manifest.transitions
        if transition.from_phase == phase_id
    }
    return sorted(targets)


# SEM_BEGIN orchestrator_v1.edge_evaluators.resolve_next_phase:v1
# type: METHOD
# use_case: Вычисляет следующую фазу по текущему status и transition table из flow manifest.
# feature:
#   - top-level graph в V1 управляется только phase statuses
#   - orchestrator/config/flow.yaml
# pre:
#   - state.current_status заполнен
#   - transition table содержит запись для phase_id + current_status
# post:
#   - возвращает target phase id или `end`
# invariant:
#   - state не мутируется
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - KeyError: pre[1] violated
# depends:
#   - FlowManifest
# sft: resolve next phase from current status using V1 transition table
# idempotent: true
# logs: query: state.current_status
def resolve_next_phase(phase_id: str, state: PipelineState, manifest: FlowManifest) -> str:
    trace_id = ensure_trace_id(state.get("trace_id"))
    current_status = state.get("current_status")

    logger.info(
        "[EdgeEvaluators][resolve_next_phase][ContextAnchor] trace_id=%s | "
        "Resolving next phase. phase=%s, status=%s",
        trace_id,
        phase_id,
        current_status,
    )

    for transition in manifest.transitions:
        if transition.from_phase == phase_id and transition.on_status == current_status:
            logger.info(
                "[EdgeEvaluators][resolve_next_phase][DecisionPoint] trace_id=%s | "
                "Branch: transition_match. Reason: phase=%s, status=%s, target=%s",
                trace_id,
                phase_id,
                current_status,
                transition.to_phase,
            )
            return transition.to_phase

    logger.warning(
        "[EdgeEvaluators][resolve_next_phase][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
        "No transition found. phase=%s, status=%s",
        trace_id,
        phase_id,
        current_status,
    )
    raise KeyError(f"No transition found for phase='{phase_id}' status='{current_status}'")


# SEM_END orchestrator_v1.edge_evaluators.resolve_next_phase:v1
