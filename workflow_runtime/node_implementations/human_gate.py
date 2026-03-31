"""Human gate node for V1 runtime."""

from __future__ import annotations

import logging
from typing import Any

from langgraph.types import interrupt

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineState, PipelineStatus
from workflow_runtime.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)


DEFAULT_HUMAN_GATE_QUESTION = "Human decision is required to continue the pipeline"


def _is_approved(response: Any) -> bool:
    if isinstance(response, dict):
        if "approved" in response:
            return bool(response["approved"])
        if "action" in response:
            return str(response["action"]).lower() in {"approve", "continue", "resume"}
    return str(response).strip().lower() in {"approve", "approved", "continue", "resume", "yes", "ok"}


# SEM_BEGIN orchestrator_v1.human_gate.run_human_gate:v1
# type: METHOD
# use_case: Pauses the graph at the human gate and converts the user decision back into PipelineState.
# feature:
#   - V1 orchestration supports interrupt/resume without losing the mutable plan
#   - Task card 2026-03-24_1800__multi-agent-system-design, D3-D5
# pre:
#   - state.trace_id or context trace_id is available
# post:
#   - returns a partial PipelineState with human_decisions and PASS/BLOCKED status
# invariant:
#   - previously accumulated human_decisions are not lost
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.langgraph_interrupt
# errors:
#   - RuntimeError: interrupt runtime failed
# depends:
#   - langgraph.types.interrupt
# sft: pause the graph for human input and convert the decision back into pipeline status
# idempotent: false
# logs: query: HumanGate trace_id
def run_human_gate(state: PipelineState) -> PipelineState:
    trace_id = ensure_trace_id(state.get("trace_id"))
    prompt_payload = state.get("pending_human_input") or {
        "source_phase": state.get("current_phase"),
        "question": DEFAULT_HUMAN_GATE_QUESTION,
    }

    logger.info(
        "[HumanGate][run_human_gate][ContextAnchor] trace_id=%s | "
        "Interrupting for human input. source_phase=%s",
        trace_id,
        prompt_payload.get("source_phase"),
    )
    response = interrupt(prompt_payload)
    approved = _is_approved(response)
    decisions = list(state.get("human_decisions", []))
    decisions.append({"prompt": prompt_payload, "response": response})

    logger.info(
        "[HumanGate][run_human_gate][DecisionPoint] trace_id=%s | "
        "Branch: human_response. Reason: approved=%s",
        trace_id,
        approved,
    )
    logger.info(
        "[HumanGate][run_human_gate][StepComplete] trace_id=%s | "
        "Human gate resolved. status=%s",
        trace_id,
        PipelineStatus.PASS if approved else PipelineStatus.BLOCKED,
    )
    return {
        "current_phase": PhaseId.HUMAN_GATE,
        "current_status": PipelineStatus.PASS if approved else PipelineStatus.BLOCKED,
        "pending_human_input": None,
        "human_decisions": decisions,
    }


# SEM_END orchestrator_v1.human_gate.run_human_gate:v1
