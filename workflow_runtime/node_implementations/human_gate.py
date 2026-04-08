"""Human gate node for V1 runtime."""

from __future__ import annotations

from typing import Any

from langgraph.types import interrupt
from lmnr import observe

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineState, PipelineStatus
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.integrations.tasks_storage import persist_human_gate_artifact


logger = get_logger(__name__)


DEFAULT_HUMAN_GATE_QUESTION = "Human decision is required to continue the pipeline"


# SEM_BEGIN orchestrator_v1.human_gate._is_approved:v1
# type: METHOD
# use_case: Normalizes human-gate responses into a boolean approval signal.
# feature:
#   - Human resume payloads may come back as booleans action dicts or free-form text from the interrupt layer
# pre:
#   -
# post:
#   - returns true only for approval/continue style responses
# invariant:
#   - response is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: normalize human gate response payloads into an approval boolean
# idempotent: true
# logs: -
def _is_approved(response: Any) -> bool:
    if isinstance(response, dict):
        if "approved" in response:
            return bool(response["approved"])
        if "action" in response:
            return str(response["action"]).lower() in {"approve", "continue", "resume"}
    return str(response).strip().lower() in {"approve", "approved", "continue", "resume", "yes", "ok"}


# SEM_END orchestrator_v1.human_gate._is_approved:v1


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
@observe(name="phase_human_gate")
def run_human_gate(state: PipelineState) -> PipelineState:
    trace_id = ensure_trace_id(state.get("trace_id"))
    prompt_payload = state.get("pending_human_input") or {
        "source_phase": state.get("current_phase"),
        "question": DEFAULT_HUMAN_GATE_QUESTION,
    }
    question_ref = state.get("pending_approval_ref") or persist_human_gate_artifact(
        task_context=state,
        phase_id=str(prompt_payload.get("source_phase") or state.get("current_phase") or PhaseId.HUMAN_GATE),
        subtask_id=prompt_payload.get("subtask_id"),
        attempt=len(state.get("human_decisions", [])) + 1,
        trace_id=trace_id,
        artifact_kind="human_gate_question",
        payload=prompt_payload,
    )

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
    human_decision_refs = list(state.get("human_decision_refs", []))
    decision_ref = persist_human_gate_artifact(
        task_context=state,
        phase_id=str(prompt_payload.get("source_phase") or state.get("current_phase") or PhaseId.HUMAN_GATE),
        subtask_id=prompt_payload.get("subtask_id"),
        attempt=int((question_ref or {}).get("attempt", len(decisions))),
        trace_id=trace_id,
        artifact_kind="human_gate_decision",
        payload={
            "prompt": prompt_payload,
            "response": response,
            "approved": approved,
        },
        summary_path=str((question_ref or {}).get("path") or "") or None,
    )
    if decision_ref is not None:
        human_decision_refs.append(decision_ref)

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
        "pending_approval_ref": None,
        "human_decisions": decisions,
        "human_decision_refs": human_decision_refs,
    }


# SEM_END orchestrator_v1.human_gate.run_human_gate:v1
