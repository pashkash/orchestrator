"""Simple V1 guardrails for universal TaskUnit."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from workflow_runtime.graph_compiler.state_schema import PipelineStatus, SubRole
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)
_CHECKLIST_RESOLUTION_STATUSES = {"done", "not_applicable", "failed", "blocked"}


# SEM_BEGIN orchestrator_v1.guardrail_checker.guardrail_result:v1
# type: CLASS
# use_case: Normalized outcome of one guardrail pass over a TaskUnit step payload.
# feature:
#   - TaskUnit must convert field/checklist validation into a status plus warning bundle
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4
# pre:
#   -
# post:
#   -
# invariant:
#   - status is always a PipelineStatus value
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PipelineStatus
# sft: define normalized guardrail result carrying status and warnings
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class GuardrailResult:
    status: PipelineStatus
    warnings: list[str] = field(default_factory=list)


# SEM_END orchestrator_v1.guardrail_checker.guardrail_result:v1


# SEM_BEGIN orchestrator_v1.guardrail_checker._failure_status:v1
# type: METHOD
# use_case: Maps a failing step to the matching NEEDS_FIX_* pipeline status.
# feature:
#   - Guardrail failures must point repair loops to the correct executor reviewer or tester branch
# pre:
#   - step_name is one of executor/reviewer/tester
# post:
#   - returns the matching repair status
# invariant:
#   - no runtime state is mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PipelineStatus
#   - SubRole
# sft: map failing task unit sub-role to the corresponding needs-fix pipeline status
# idempotent: true
# logs: -
def _failure_status(step_name: SubRole) -> PipelineStatus:
    if step_name == SubRole.REVIEWER:
        return PipelineStatus.NEEDS_FIX_REVIEW
    if step_name == SubRole.TESTER:
        return PipelineStatus.NEEDS_FIX_TESTS
    return PipelineStatus.NEEDS_FIX_EXECUTOR


# SEM_END orchestrator_v1.guardrail_checker._failure_status:v1


# SEM_BEGIN orchestrator_v1.guardrail_checker._required_keys:v1
# type: METHOD
# use_case: Returns the minimal payload keys required for one phase/sub-role contract.
# feature:
#   - Guardrails need phase-aware contract checks because collect plan validate and worker execution return different payload shapes
# pre:
#   - step_name is one of executor/reviewer/tester
# post:
#   - returns the required key list for that phase/sub-role
# invariant:
#   - no runtime state is mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - SubRole
# sft: derive required payload keys for one phase and task unit sub-role
# idempotent: true
# logs: -
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


# SEM_END orchestrator_v1.guardrail_checker._required_keys:v1


# SEM_BEGIN orchestrator_v1.guardrail_checker._extract_unchecked_boxes:v1
# type: METHOD
# use_case: Extracts all unchecked markdown checklist lines from one task artifact.
# feature:
#   - Checklist guardrail must validate real task memory markdown instead of a synthetic boolean flag
#   - Task card 2026-03-24_1800__multi-agent-system-design, D9
# pre:
#   - path points to a markdown artifact when it exists
# post:
#   - returns all lines that still contain unchecked checklist items
# invariant:
#   - filesystem is accessed in readonly mode
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - Path.read_text
# sft: read one markdown task artifact and collect unchecked checklist lines for guardrail validation
# idempotent: true
# logs: -
def _extract_unchecked_boxes(path: Path) -> list[str]:
    if not path.exists() or not path.is_file():
        return []
    unchecked: list[str] = []
    for raw_line in path.read_text().splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("- [ ]") or stripped.startswith("* [ ]"):
            unchecked.append(stripped)
    return unchecked


# SEM_END orchestrator_v1.guardrail_checker._extract_unchecked_boxes:v1


# SEM_BEGIN orchestrator_v1.guardrail_checker._check_task_artifact_checklist:v1
# type: METHOD
# use_case: Validates whether task/subtask markdown artifacts still contain unchecked checklist items.
# feature:
#   - Runtime checklist enforcement must operate on real TASK.md and subtask cards
#   - Task card 2026-03-24_1800__multi-agent-system-design, D9
# pre:
#   -
# post:
#   - returns warning messages for missing artifacts or open checklist items
# invariant:
#   - task_context is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - _extract_unchecked_boxes
# sft: validate task and subtask markdown artifacts for unresolved checklist items
# idempotent: true
# logs: -
def _check_task_artifact_checklist(task_context: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    task_card_path = str(task_context.get("task_card_path") or "").strip()
    subtask_card_path = str(task_context.get("subtask_card_path") or "").strip()
    artifact_paths: list[tuple[str, Path]] = []
    if subtask_card_path and Path(subtask_card_path).exists():
        artifact_paths = [("subtask_card", Path(subtask_card_path))]
    elif task_card_path and Path(task_card_path).exists():
        artifact_paths = [("task_card", Path(task_card_path))]

    if not artifact_paths:
        if subtask_card_path or task_card_path:
            return ["Checklist guardrail requires an existing task_card_path or subtask_card_path"]
        return []

    for label, path in artifact_paths:
        unchecked = _extract_unchecked_boxes(path)
        if unchecked:
            preview = "; ".join(unchecked[:3])
            suffix = " ..." if len(unchecked) > 3 else ""
            warnings.append(
                f"Unchecked checklist items remain in {label}={path}: {preview}{suffix}"
            )
    return warnings


# SEM_END orchestrator_v1.guardrail_checker._check_task_artifact_checklist:v1


def _check_prompt_checklist_coverage(
    *,
    payload: dict[str, Any],
    task_context: dict[str, Any],
) -> list[str]:
    expected_items = task_context.get("guardrail_prompt_checklists")
    if not isinstance(expected_items, list) or not expected_items:
        return []

    raw_resolutions = payload.get("checklist_resolutions")
    if not isinstance(raw_resolutions, list):
        return [
            "checklist_resolutions must be a list covering all checklist items from role/common/standards sources"
        ]

    warnings: list[str] = []
    resolution_by_id: dict[str, dict[str, Any]] = {}
    for entry in raw_resolutions:
        if not isinstance(entry, dict):
            warnings.append("Each checklist_resolutions entry must be a mapping")
            continue
        item_id = str(entry.get("id") or "").strip()
        if not item_id:
            warnings.append("Each checklist_resolutions entry must include a non-empty id")
            continue
        if item_id in resolution_by_id:
            warnings.append(f"Duplicate checklist_resolutions id: {item_id}")
            continue
        resolution_by_id[item_id] = entry

    missing_ids = [str(item.get("id") or "").strip() for item in expected_items if str(item.get("id") or "").strip() not in resolution_by_id]
    if missing_ids:
        preview = ", ".join(missing_ids[:5])
        suffix = " ..." if len(missing_ids) > 5 else ""
        warnings.append(f"Missing checklist_resolutions for checklist item ids: {preview}{suffix}")

    payload_status = str(payload.get("status") or "").strip().upper()
    for item in expected_items:
        item_id = str(item.get("id") or "").strip()
        if not item_id or item_id not in resolution_by_id:
            continue
        resolution = resolution_by_id[item_id]
        raw_status = str(resolution.get("status") or "").strip().lower()
        if raw_status not in _CHECKLIST_RESOLUTION_STATUSES:
            warnings.append(
                f"Checklist item {item_id} has invalid resolution status: {resolution.get('status')}"
            )
        evidence = str(
            resolution.get("evidence")
            or resolution.get("reason")
            or ""
        ).strip()
        if not evidence:
            warnings.append(f"Checklist item {item_id} must include non-empty evidence")
        if payload_status == PipelineStatus.PASS and raw_status in {"failed", "blocked"}:
            warnings.append(
                f"Checklist item {item_id} is marked {raw_status} while payload status is PASS"
            )
    return warnings


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

    _KEY_ALIASES = {"verdict": "status", "response": "feedback", "review": "feedback", "result": "result"}
    for alias, canonical in _KEY_ALIASES.items():
        if alias in payload and canonical not in payload:
            payload[canonical] = payload[alias]

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
        elif guardrail_name == "ensure_checklist":
            warnings.extend(_check_task_artifact_checklist(task_context))
            warnings.extend(
                _check_prompt_checklist_coverage(
                    payload=payload,
                    task_context=task_context,
                )
            )
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
