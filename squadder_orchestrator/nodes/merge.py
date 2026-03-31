"""Merge node: aggregates StructuredOutputs from all executors.

Pure code — no LLM calls. Deterministic aggregation of changes, commits,
warnings, and escalation from all subtask outputs.
"""

from __future__ import annotations

import logging

from squadder_orchestrator.state import AgentState, FileChange


logger = logging.getLogger(__name__)


# SEM_BEGIN orchestrator.merge.merge_outputs:v1
# type: METHOD
# use_case: Агрегирует все StructuredOutput из subtask-ов. Собирает commits, warnings,
#   escalation. Обнаруживает конфликты: один файл изменён разными ролями.
# feature:
#   - Merge — детерминированная нода без LLM
#   - docs/common/roles/flow.yaml — nodes.merge
# pre:
#   - state.structured_outputs — список StructuredOutput от executor-ов
# post:
#   - commits содержит форматированный список коммитов из всех outputs
#   - warnings содержит предупреждения + конфликты (файл изменён несколькими ролями)
#   - cross_cutting_result и final_result обнулены для validate
# invariant:
#   - structured_outputs не мутируются
# modifies (internal): -
# emits (external): -
# errors: -
# depends: -
# sft: aggregate all StructuredOutputs, detect file-level conflicts between roles, prepare for validation
# idempotent: true
# logs: -
def merge_outputs(state: AgentState) -> dict:
    outputs = state.get("structured_outputs", [])

    all_changes: list[FileChange] = []
    all_commits: list[str] = []
    all_warnings: list[str] = []
    escalations: list[dict] = []

    file_owners: dict[str, list[str]] = {}

    for out in outputs:
        all_commits.extend(
            f"{c.get('repo', '?')}:{c.get('hash', '?')} — {c.get('message', '?')}"
            for c in out.commits
        )
        all_warnings.extend(out.warnings)
        if out.escalation:
            escalations.append(out.escalation)

        for ch in out.changes:
            all_changes.append(ch)
            file_owners.setdefault(ch.file, []).append(out.role)

    # === POST: detect conflicts ===
    conflicts = {
        f: roles for f, roles in file_owners.items()
        if len(roles) > 1
    }
    if conflicts:
        for f, roles in conflicts.items():
            all_warnings.append(
                f"CONFLICT: {f} modified by {', '.join(roles)}"
            )
        logger.warning(
            "[Merge][merge_outputs][DecisionPoint] | "
            "Branch: conflicts_detected. Reason: %d files modified by multiple roles. files=%s",
            len(conflicts), list(conflicts.keys()),
        )

    return {
        "commits": all_commits,
        "cross_cutting_result": None,
        "final_result": None,
    }
# SEM_END orchestrator.merge.merge_outputs:v1
