"""Shared YAML contract parsing helpers for runtime drivers."""

from __future__ import annotations

import re
from typing import Any

import yaml

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineStatus, SubRole
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger

logger = get_logger(__name__)

_YAML_BLOCK_RE = re.compile(r"```(?:yaml|yml)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_TAGGED_SECTION_RE = re.compile(
    r"<(?P<tag>[a-zA-Z_][\w-]*)(?:\s+[^>]*)?>\s*(?P<body>.*?)\s*</(?P=tag)>",
    re.DOTALL | re.IGNORECASE,
)

_STATUS_ALIASES: dict[str, PipelineStatus] = {
    "done": PipelineStatus.PASS,
    "success": PipelineStatus.PASS,
    "ok": PipelineStatus.PASS,
    "completed": PipelineStatus.PASS,
    "ready": PipelineStatus.PASS,
    "finished": PipelineStatus.PASS,
    "failed": PipelineStatus.NEEDS_FIX_EXECUTOR,
    "error": PipelineStatus.NEEDS_FIX_EXECUTOR,
    "fix": PipelineStatus.NEEDS_FIX_EXECUTOR,
}

_FLAT_EXECUTOR_STRUCTURED_OUTPUT_KEYS: tuple[str, ...] = (
    "task_id",
    "subtask_id",
    "role",
    "status",
    "changes",
    "commands_executed",
    "tests_passed",
    "commits",
    "warnings",
    "escalation",
    "summary",
)

_TESTER_PASS_STATUS_ALIASES: set[str] = {
    "pass",
    "done",
    "success",
    "ok",
    "completed",
    "ready",
    "finished",
}


# SEM_BEGIN orchestrator_v1.yaml_contract.load_yaml_fragment:v1
# type: METHOD
# use_case: Пытается распарсить один YAML fragment из raw текста модели.
# feature:
#   - Driver parser должен уметь безопасно отличать "не YAML" от parseable payload без падения всего runtime
# pre:
#   - raw_text may be empty or invalid yaml
# post:
#   - returns parsed python object or None when yaml parsing failed
# invariant:
#   - raw_text is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - yaml.safe_load
# sft: safely parse one yaml fragment and return none when the fragment is invalid
# idempotent: true
# logs: query: yaml_contract _load_yaml_fragment
def _load_yaml_fragment(raw_text: str) -> Any:
    candidate = raw_text.strip()
    if not candidate:
        return None
    blocks = _YAML_BLOCK_RE.findall(candidate)
    yaml_candidate = blocks[-1] if blocks else candidate
    try:
        return yaml.safe_load(yaml_candidate)
    except yaml.YAMLError as exc:
        trace_id = ensure_trace_id()
        logger.warning(
            "[YamlContract][_load_yaml_fragment][ErrorHandled][ERR:DATA_INTEGRITY] trace_id=%s | "
            "YAML fragment parse failed. candidate_len=%d, error=%s",
            trace_id,
            len(yaml_candidate),
            str(exc),
        )
        return None
# SEM_END orchestrator_v1.yaml_contract.load_yaml_fragment:v1


def _merge_tagged_yaml_sections(raw_text: str, payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    for match in _TAGGED_SECTION_RE.finditer(raw_text):
        tag = str(match.group("tag") or "").strip().lower()
        loaded = _load_yaml_fragment(match.group("body") or "")
        if tag == "structured_output" and isinstance(loaded, dict):
            merged["structured_output"] = loaded
            merged.setdefault("status", str(loaded.get("status") or PipelineStatus.PASS))
        elif tag == "checklist_resolutions" and isinstance(loaded, list):
            merged["checklist_resolutions"] = loaded
    return merged


# SEM_BEGIN orchestrator_v1.yaml_contract.coerce_payload:v1
# type: METHOD
# use_case: Извлекает первый пригодный YAML payload из raw LLM text и tagged sections.
# feature:
#   - Driver-ы должны уметь читать YAML и из fenced block, и из plain text, и из structured_output/checklist_resolutions tags
# pre:
#   - raw_text may contain prose, fenced yaml, or tagged sections
# post:
#   - returns merged payload dict or None when no parseable YAML dict exists
# invariant:
#   - raw_text is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _load_yaml_fragment
#   - _merge_tagged_yaml_sections
# sft: parse a runtime driver payload from raw llm text and merge tagged yaml sections
# idempotent: true
# logs: -
def coerce_payload(raw_text: str) -> dict[str, Any] | None:
    blocks = _YAML_BLOCK_RE.findall(raw_text)
    candidates = list(reversed(blocks)) if blocks else [raw_text]
    for candidate in candidates:
        loaded = _load_yaml_fragment(candidate)
        if isinstance(loaded, dict):
            return _merge_tagged_yaml_sections(raw_text, loaded)
    return None
# SEM_END orchestrator_v1.yaml_contract.coerce_payload:v1


# SEM_BEGIN orchestrator_v1.yaml_contract.normalize_payload_shape:v1
# type: METHOD
# use_case: Нормализует LLM payload в shape, ожидаемый runtime contract-ами executor/reviewer/tester.
# feature:
#   - Старые/плоские payload shapes должны автоматически приводиться к современному structured schema без переписывания всех prompt-ов сразу
# pre:
#   - payload is None or dict-like model output
# post:
#   - returns original payload or normalized dict aligned with sub_role/phase contract
# invariant:
#   - semantic meaning of status/result/structured_output is preserved
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - PhaseId
#   - SubRole
# sft: normalize runtime payloads for executor reviewer and tester steps into the expected contract shape
# idempotent: true
# logs: -
def normalize_payload_shape(
    phase_id: PhaseId | str,
    sub_role: SubRole,
    payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return payload
    if sub_role == SubRole.TESTER and not isinstance(payload.get("result"), dict):
        required_flat_keys = {
            "task_id",
            "subtask_id",
            "role",
            "tests_passed",
            "summary",
        }
        if required_flat_keys.issubset(payload.keys()):
            normalized_payload = dict(payload)
            raw_status = str(payload.get("status") or PipelineStatus.PASS)
            diagnostics = None
            if raw_status.strip().lower() not in _TESTER_PASS_STATUS_ALIASES:
                summary = str(payload.get("summary") or "").strip()
                diagnostics = summary or None
            normalized_payload["status"] = raw_status
            normalized_payload["result"] = {
                "tests": [
                    {
                        "name": f"check_{index}",
                        "status": "pass",
                        "output": str(test_name),
                    }
                    for index, test_name in enumerate(payload.get("tests_passed", []), start=1)
                ],
                "diagnostics": diagnostics,
            }
            if "feedback" not in normalized_payload:
                normalized_payload["feedback"] = str(payload.get("summary") or "").strip()
            return normalized_payload
        return payload
    if sub_role != SubRole.EXECUTOR:
        return payload
    if str(phase_id) in {"collect", "plan", "validate"}:
        return payload
    if isinstance(payload.get("structured_output"), dict):
        return payload

    required_flat_keys = {
        "task_id",
        "subtask_id",
        "role",
        "changes",
        "commands_executed",
        "tests_passed",
        "commits",
        "summary",
    }
    if not required_flat_keys.issubset(payload.keys()):
        return payload

    structured_output = {
        key: payload[key] for key in _FLAT_EXECUTOR_STRUCTURED_OUTPUT_KEYS if key in payload
    }
    normalized_payload = {
        key: value
        for key, value in payload.items()
        if key not in _FLAT_EXECUTOR_STRUCTURED_OUTPUT_KEYS
    }
    normalized_payload["status"] = str(payload.get("status") or PipelineStatus.PASS)
    normalized_payload["structured_output"] = structured_output
    if "warnings" in structured_output and "warnings" not in normalized_payload:
        normalized_payload["warnings"] = structured_output["warnings"]
    return normalized_payload
# SEM_END orchestrator_v1.yaml_contract.normalize_payload_shape:v1


def status_for_parse_failure(sub_role: SubRole) -> PipelineStatus:
    if sub_role == SubRole.REVIEWER:
        return PipelineStatus.NEEDS_FIX_REVIEW
    if sub_role == SubRole.TESTER:
        return PipelineStatus.NEEDS_FIX_TESTS
    return PipelineStatus.NEEDS_FIX_EXECUTOR


# SEM_BEGIN orchestrator_v1.yaml_contract.normalize_status:v1
# type: METHOD
# use_case: Приводит raw status string из model output к canonical PipelineStatus.
# feature:
#   - prompt-ы могут возвращать alias values вроде done/success/error; runtime должен переводить их в stable graph vocabulary
# pre:
#   - raw_status is a string-like value
# post:
#   - returns a PipelineStatus enum suitable for phase routing
# invariant:
#   - unknown statuses degrade to parse-failure status for the current sub_role
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - status_for_parse_failure
# sft: normalize raw model status text into the canonical pipeline status enum
# idempotent: true
# logs: -
def normalize_status(raw_status: str, sub_role: SubRole) -> PipelineStatus:
    upper = raw_status.strip().upper()
    try:
        return PipelineStatus(upper)
    except ValueError:
        alias = _STATUS_ALIASES.get(raw_status.strip().lower())
        if alias:
            return alias
        return status_for_parse_failure(sub_role)
# SEM_END orchestrator_v1.yaml_contract.normalize_status:v1


# SEM_BEGIN orchestrator_v1.yaml_contract.extract_text_content:v1
# type: METHOD
# use_case: Извлекает нормализованный text body из provider response content в нескольких transport shape-ах.
# feature:
#   - Runtime drivers должны одинаково читать plain string, list fragments и dict{text=...} payloads
# pre:
#   - content may be str, list, dict, or provider-specific object-like payload
# post:
#   - returns normalized text content or empty string
# invariant:
#   - content container shape is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   -
# sft: extract normalized plain text from provider response content across string list and dict payload shapes
# idempotent: true
# logs: -
def extract_text_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        fragments: list[str] = []
        for item in content:
            if isinstance(item, str) and item.strip():
                fragments.append(item.strip())
                continue
            if isinstance(item, dict):
                text_value = item.get("text")
                if isinstance(text_value, str) and text_value.strip():
                    fragments.append(text_value.strip())
        return "\n\n".join(fragments).strip()
    if isinstance(content, dict):
        text_value = content.get("text")
        if isinstance(text_value, str):
            return text_value.strip()
    return ""
# SEM_END orchestrator_v1.yaml_contract.extract_text_content:v1


# SEM_BEGIN orchestrator_v1.yaml_contract.resolve_provider_model_name:v1
# type: METHOD
# use_case: Нормализует provider model name под transport-specific naming quirks.
# feature:
#   - OpenRouter base_url требует удаления `openrouter/` prefix, while other providers keep the original model name
# pre:
#   - model and base_url are string-like
# post:
#   - returns provider-ready model name
# invariant:
#   - non-OpenRouter providers keep the original model identifier
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   -
# sft: normalize the configured model name for provider-specific transport requirements such as openrouter prefixes
# idempotent: true
# logs: -
def resolve_provider_model_name(model: str, base_url: str) -> str:
    normalized_model = model.strip()
    if "openrouter" in base_url and normalized_model.startswith("openrouter/"):
        return normalized_model[len("openrouter/") :]
    return normalized_model
# SEM_END orchestrator_v1.yaml_contract.resolve_provider_model_name:v1


# SEM_BEGIN orchestrator_v1.yaml_contract.required_payload_keys:v1
# type: METHOD
# use_case: Возвращает минимальный top-level YAML contract для конкретного phase/sub_role.
# feature:
#   - Drivers используют этот contract для parse validation, repair prompts и formatter fallback
# pre:
#   - phase_id and sub_role refer to a known runtime step shape
# post:
#   - returns required top-level keys for that step
# invariant:
#   - validator vocabulary stays aligned with runtime parsing logic
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - SubRole
# sft: determine which top-level yaml keys a runtime step must return
# idempotent: true
# logs: -
def required_payload_keys(phase_id: PhaseId | str, sub_role: SubRole) -> list[str]:
    normalized_phase = str(phase_id)
    if sub_role == SubRole.REVIEWER:
        return ["status", "feedback"]
    if sub_role == SubRole.TESTER:
        return ["status", "result"]
    if normalized_phase == "collect":
        return ["status", "current_state"]
    if normalized_phase == "plan":
        return ["status", "plan"]
    if normalized_phase == "validate":
        return ["status", "cross_cutting_result"]
    return ["status", "structured_output"]
# SEM_END orchestrator_v1.yaml_contract.required_payload_keys:v1


# SEM_BEGIN orchestrator_v1.yaml_contract.missing_required_payload_keys:v2
# type: METHOD
# use_case: Возвращает список обязательных ключей, которых не хватает в parsed payload, включая checklist contract при active guardrail items.
# feature:
#   - Drivers используют это для repair prompt и formatter fallback decision path
#   - ensure_checklist шаги должны уметь автоматически допросить модель про отсутствующий checklist_resolutions, а не падать только на post-driver guardrail
# pre:
#   - payload may be None or dict
# post:
#   - returns missing required keys for the given phase/sub_role contract
# invariant:
#   - base required key set comes from required_payload_keys for the same phase/sub_role
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - required_payload_keys
# sft: report which required yaml keys are missing from a parsed payload for the given runtime step, including checklist coverage keys when the prompt advertised checklist items
# idempotent: true
# logs: -
def missing_required_payload_keys(
    phase_id: PhaseId | str,
    sub_role: SubRole,
    payload: dict[str, Any] | None,
    task_context: dict[str, Any] | None = None,
) -> list[str]:
    missing = required_payload_keys(phase_id, sub_role)
    if isinstance(payload, dict):
        missing = [key for key in missing if key not in payload]
    checklist_items = (task_context or {}).get("guardrail_prompt_checklists")
    has_checklist_contract = isinstance(checklist_items, list) and bool(checklist_items)
    if not has_checklist_contract:
        return missing
    if not isinstance(payload, dict) or not isinstance(payload.get("checklist_resolutions"), list):
        return [*missing, "checklist_resolutions"]
    return missing
# SEM_END orchestrator_v1.yaml_contract.missing_required_payload_keys:v2
