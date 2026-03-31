"""Fan-out router: dispatches subtasks by dependency order.

Pure code — no LLM calls. Reads plan from state and groups subtasks
into execution waves based on dependency DAG.
"""

from __future__ import annotations

import logging

from squadder_orchestrator.state import AgentState, Subtask


logger = logging.getLogger(__name__)



# SEM_BEGIN orchestrator.router.get_ready_subtasks:v1
# type: METHOD
# use_case: Возвращает subtask-и, готовые к исполнению: status == "pending" и все зависимости
#   в статусе "done". Используется как items_fn для fan-out и как condition predicate
#   has_ready_subtasks.
# feature:
#   - Зарегистрирован в conditions как items_fn "get_ready_subtasks"
#   - Зарегистрирован как condition_fn "has_ready_subtasks" (len > 0)
# pre:
#   - state.plan — список Subtask
# post:
#   - возвращает list[Subtask] — готовые к исполнению
# invariant:
#   - plan не мутируется
# modifies (internal): -
# emits (external): -
# errors: -
# depends: -
# sft: filter subtasks ready for execution — pending status with all dependencies done
# idempotent: true
# logs: -
def get_ready_subtasks(state: AgentState) -> list[Subtask]:
    plan = state.get("plan", [])
    done_ids = {
        st.id for st in plan
        if st.status == "done"
    }
    return [
        st for st in plan
        if st.status == "pending"
        and all(dep in done_ids for dep in st.dependencies)
    ]
# SEM_END orchestrator.router.get_ready_subtasks:v1
