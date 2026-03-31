"""Simple V1 guardrails for universal TaskUnit."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from workflow_runtime.graph_compiler.state_schema import PipelineStatus, SubRole
from workflow_runtime.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GuardrailResult:
    status: PipelineStatus
    warnings: list[str] = field(default_factory=list)


def _failure_status(step_name: SubRole) -> PipelineStatus:
    if step_name == SubRole.REVIEWER:
        return PipelineStatus.NEEDS_FIX_REVIEW
    if step_name == SubRole.TESTER:
        return PipelineStatus.NEEDS_FIX_TESTS
    return PipelineStatus.NEEDS_FIX_EXECUTOR


def _required_keys(phase_id: str, step_name: SubRole) -> list[str]:
    if step_name == SubRole.REVIEWER:
        return ["status", "feedback"]
    if step_name == SubRole.TESTER:
        return ["status", "result"]
    if phase_id == "collect":
        return ["status", "current_state"]
    if phase_id == "plan":
        return ["status", "plan"]
    if phase_id == "validate":
        return ["status", "cross_cutting_result"]
    return ["status", "structured_output"]


# SEM_BEGIN orchestrator_v1.guardrail_checker.run_guardrails:v1
# type: METHOD
# use_case: Applies a simple set of V1 guardrails to a TaskUnit step result.
# feature:
#   - V1 guardrails only check required fields, checklists, and the basic StructuredOutput shape
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0/D4
# pre:
#   - payload is a dict from the driver step
#   - step_name is one of executor/reviewer/tester
# post:
#   - returns a GuardrailResult with PASS or NEEDS_FIX_* status
# invariant:
#   - payload and task_context are not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: validate one task unit step payload with simple required-field and checklist guardrails
# idempotent: true
# logs: query: guardrail warnings for missing required keys
def run_guardrails(
    *,
    phase_id: str,
    step_name: SubRole,
    payload: dict[str, Any],
    guardrails: list[str],
    task_context: dict[str, Any],
    trace_id: str | None = None,
) -> GuardrailResult:
    resolved_trace_id = ensure_trace_id(trace_id)
    warnings: list[str] = []

    logger.info(
        "[GuardrailChecker][run_guardrails][ContextAnchor] trace_id=%s | "
        "Applying guardrails. phase=%s, step=%s, guardrails=%d",
        resolved_trace_id,
        phase_id,
        step_name,
        len(guardrails),
    )

    for guardrail_name in guardrails:
        if guardrail_name in {"ensure_required_fields", "ensure_status_field", "ensure_feedback_field"}:
            for key in _required_keys(phase_id, step_name):
                if key not in payload:
                    warnings.append(f"Missing required key: {key}")
        elif guardrail_name == "ensure_non_empty_payload" and not payload:
                warnings.append("Payload is empty")
        elif guardrail_name == "ensure_plan_payload" and not isinstance(payload.get("plan"), list):
            warnings.append("Plan payload must be a list")
        elif guardrail_name == "ensure_validate_payload" and "cross_cutting_result" not in payload:
            warnings.append("Validate payload must contain cross_cutting_result")
        elif guardrail_name == "ensure_structured_output":
            structured_output = payload.get("structured_output")
            if not isinstance(structured_output, dict):
                warnings.append("structured_output must be a mapping")
            else:
                for key in [
                    "task_id",
                    "subtask_id",
                    "role",
                    "status",
                    "changes",
                    "commands_executed",
                    "tests_passed",
                    "commits",
                    "warnings",
                    "summary",
                ]:
                    if key not in structured_output:
                        warnings.append(f"structured_output missing key: {key}")
        elif guardrail_name == "ensure_checklist" and task_context.get("checklist_ok") is False:
            warnings.append("Checklist is not completed")
        elif guardrail_name == "ensure_tests_summary" and not payload.get("tests_passed") and not payload.get("result"):
            warnings.append("Tester payload must include tests summary")

    if warnings:
        logger.warning(
            "[GuardrailChecker][run_guardrails][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Guardrails failed. phase=%s, step=%s, warnings=%s",
            resolved_trace_id,
            phase_id,
            step_name,
            warnings,
        )
        return GuardrailResult(status=_failure_status(step_name), warnings=warnings)

    logger.info(
        "[GuardrailChecker][run_guardrails][StepComplete] trace_id=%s | "
        "Guardrails passed. phase=%s, step=%s",
        resolved_trace_id,
        phase_id,
        step_name,
    )
    return GuardrailResult(status=PipelineStatus.PASS)


# SEM_END orchestrator_v1.guardrail_checker.run_guardrails:v1
