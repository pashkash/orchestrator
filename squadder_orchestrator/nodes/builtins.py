"""Built-in node implementations for the orchestrator graph.

These are generic utilities not tied to any specific role or LLM.
They handle state bookkeeping: collecting results, retry counters,
subtask lifecycle, and human gate placeholder.
"""

from __future__ import annotations

import logging

from squadder_orchestrator.state import StructuredOutput


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main graph builtins
# ---------------------------------------------------------------------------

# SEM_BEGIN orchestrator.builtins.collect_results:v1
# type: METHOD
# use_case: Собирает новые StructuredOutput из завершённых subtask-ов плана.
#   Дедуплицирует по subtask_id чтобы не считать повторно при multi-wave fan-out
#   (Annotated[list, add] на AgentState.structured_outputs конкатенирует).
# feature:
#   - Деdup нужен из-за того что collect_results вызывается после каждой волны
#   - docs/common/roles/flow.yaml — edges collect_results.routes
# pre:
#   - state.plan — список Subtask с заполненными structured_output для done/failed
# post:
#   - возвращает {"structured_outputs": [новые outputs]} — без дубликатов по subtask_id
# invariant:
#   - plan не мутируется
# modifies (internal): -
# emits (external): -
# errors: -
# depends: -
# sft: collect new StructuredOutputs from finished subtasks, deduplicate by subtask_id
# idempotent: true
# logs: -
def collect_results(state: dict) -> dict:
    plan = state.get("plan", [])
    existing_ids = {out.subtask_id for out in state.get("structured_outputs", [])}
    new_outputs: list[StructuredOutput] = []
    for subtask in plan:
        if subtask.id in existing_ids:
            continue
        if subtask.structured_output and subtask.status in ("done", "failed"):
            new_outputs.append(subtask.structured_output)
    return {"structured_outputs": new_outputs}
# SEM_END orchestrator.builtins.collect_results:v1


def human_gate(state: dict) -> dict:
    """Placeholder for human-in-the-loop. Auto-approves in Phase 2."""
    return {
        "human_decisions": state.get("human_decisions", []) + [
            {"question": "Auto-approved (mock)", "answer": "approved", "timestamp": "mock"},
        ],
    }


# ---------------------------------------------------------------------------
# Subtask subgraph builtins
# ---------------------------------------------------------------------------

# SEM_BEGIN orchestrator.builtins.subtask_done:v1
# type: METHOD
# use_case: Финализирует subtask как done. Копирует executor_output, review_verdict,
#   review_feedback и test_result из SubtaskExecState в объект Subtask.
# feature:
#   - Конечная нода subgraph subtask_execution при успехе
#   - docs/common/roles/flow.yaml — subgraphs.subtask_execution.nodes.done
# pre:
#   - state["subtask"] существует
#   - state["executor_output"] заполнен
# post:
#   - subtask.status == "done"
#   - subtask.structured_output, reviewer_verdict, reviewer_feedback, tester_result заполнены
#   - возвращает {"final_status": "done"}
# invariant: -
# modifies (internal): -
# emits (external): -
# errors: -
# depends: -
# sft: mark subtask as done, copy execution results from subgraph state to subtask object
# idempotent: false
# logs: -
def subtask_done(state: dict) -> dict:
    subtask = state["subtask"]
    subtask.status = "done"
    subtask.structured_output = state.get("executor_output")
    subtask.reviewer_verdict = state.get("review_verdict")
    subtask.reviewer_feedback = state.get("review_feedback")
    subtask.tester_result = state.get("test_result")
    return {"final_status": "done"}
# SEM_END orchestrator.builtins.subtask_done:v1


# SEM_BEGIN orchestrator.builtins.subtask_fail:v1
# type: METHOD
# brief: Помечает subtask как failed. Зеркало subtask_done для аварийного пути.
# pre:
#   - state["subtask"] — объект Subtask
# post:
#   - subtask.status == "failed"
#   - executor_output.status == "failed" (если output есть)
#   - возвращает {"final_status": "failed"}
# invariant: -
# modifies (internal): -
# emits (external): -
# errors: -
# depends: -
# sft: mark subtask as failed, propagate failure status to executor output
# idempotent: false
# logs: -
def subtask_fail(state: dict) -> dict:
    subtask = state["subtask"]
    subtask.status = "failed"
    output = state.get("executor_output")
    if output:
        output.status = "failed"
    return {"final_status": "failed"}
# SEM_END orchestrator.builtins.subtask_fail:v1


def increment_retry(state: dict) -> dict:
    """Bump retry counter for subtask re-execution."""
    return {"retry_count": state.get("retry_count", 0) + 1}
