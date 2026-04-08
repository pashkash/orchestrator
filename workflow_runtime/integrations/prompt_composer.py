"""Prompt composition utilities for V1 runtime manifests.

The orchestrator's job is minimal: read the prompt file specified in
phases_and_roles.yaml, force-inject only the mandatory methodology docs
configured globally and per role, append the runtime task context (paths, IDs),
and append the output contract so the driver can parse the response.

All detailed behavioral instructions still live in the docs and prompt
markdown files themselves. The orchestrator only glues those sources
together; it does not invent strategy-specific business instructions.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
import re
from typing import Any

import yaml

from workflow_runtime.graph_compiler.state_schema import PhaseId
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import (
    get_docs_root,
    get_methodology_root_runtime,
    get_role_metadata_path,
    get_runtime_config,
    load_role_metadata,
    resolve_methodology_entrypoint,
    resolve_runtime_path,
)
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)

_CHECKLIST_LINE_PATTERN = re.compile(r"^(?:-|\*) \[ \] .+")
_DOC_REFERENCE_PATTERNS = (
    re.compile(r"\[\[([^\]]+\.(?:md|ya?ml)(?:#[^\]]+)?)\]\]"),
    re.compile(r"`([^`\n]+\.(?:md|ya?ml)(?:#[^`\n]+)?)`"),
    re.compile(r"<!--\s*include:\s*([^\s>]+\.(?:md|ya?ml))\s*-->"),
)
_CHECKLIST_STATUS_VALUES = ("done", "not_applicable", "failed", "blocked")

def _read_markdown(path: Path) -> str:
    return path.read_text()


def _render_context_value(value: Any) -> list[str]:
    if isinstance(value, (dict, list)):
        rendered = yaml.safe_dump(_normalize_context_value(value), sort_keys=False, allow_unicode=False).rstrip()
        return rendered.splitlines() if rendered else ["null"]
    return [str(value)]


def _normalize_context_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _normalize_context_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_context_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_context_value(item) for item in value]
    return value


def _render_context(task_context: dict[str, Any]) -> str:
    if not task_context:
        return "none"
    lines = []
    for key, value in task_context.items():
        rendered_lines = _render_context_value(value)
        if len(rendered_lines) == 1 and not isinstance(value, (dict, list)):
            lines.append(f"- {key}: {rendered_lines[0]}")
            continue
        lines.append(f"- {key}:")
        lines.extend(f"  {line}" for line in rendered_lines)
    return "\n".join(lines)


def _resolve_document_reference(reference: str, *, role_dir: str) -> Path | None:
    normalized = reference.strip().split("#", 1)[0]
    if not normalized.endswith((".md", ".yaml", ".yml")):
        return None
    if normalized.startswith("/") or normalized.startswith("Technical Docs/"):
        return resolve_runtime_path(normalized, role_dir).resolve()
    return (get_docs_root() / normalized.replace("{role_dir}", role_dir)).resolve()


def _resolve_document_references(
    references: list[str],
    *,
    role_dir: str,
    strict: bool,
) -> list[Path]:
    ordered_paths: list[Path] = []
    visited: set[Path] = set()
    for reference in references:
        resolved = _resolve_document_reference(reference, role_dir=role_dir)
        if resolved is None:
            if strict:
                raise FileNotFoundError(f"Configured document is not a supported docs file: {reference}")
            continue
        if resolved in visited:
            continue
        if not resolved.exists() or not resolved.is_file():
            if strict:
                raise FileNotFoundError(f"Configured document not found: {resolved}")
            continue
        visited.add(resolved)
        ordered_paths.append(resolved)
    return ordered_paths


def _is_guardrail_checklist_source(path: Path) -> bool:
    docs_root = get_docs_root().resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(docs_root)
    except ValueError:
        return False
    relative_text = relative.as_posix()
    return relative_text.startswith("common/roles/") or relative_text.startswith("common/standards/")


def _extract_document_references_from_text(text: str) -> list[str]:
    references: list[str] = []
    for pattern in _DOC_REFERENCE_PATTERNS:
        references.extend(match.group(1).strip() for match in pattern.finditer(text))
    return references


def _resolve_embedded_document_reference(
    reference: str,
    *,
    current_path: Path,
    role_dir: str,
) -> Path | None:
    normalized = reference.strip().split("#", 1)[0]
    if not normalized.endswith((".md", ".yaml", ".yml")):
        return None
    if normalized.startswith("/"):
        return Path(normalized).resolve()
    if normalized.startswith("Technical Docs/"):
        return resolve_runtime_path(normalized, role_dir).resolve()
    if normalized.startswith("./") or normalized.startswith("../"):
        return (current_path.parent / normalized).resolve()
    return (get_docs_root() / normalized.replace("{role_dir}", role_dir)).resolve()


def _extract_unchecked_checklist_entries(path: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    in_fence = False
    docs_root = get_docs_root().resolve()
    relative_source = path.resolve().relative_to(docs_root).as_posix()
    for line_no, raw_line in enumerate(path.read_text().splitlines(), start=1):
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence or not _CHECKLIST_LINE_PATTERN.match(stripped):
            continue
        entries.append(
            {
                "id": f"checklist::{relative_source}::L{line_no}",
                "source": relative_source,
                "line": line_no,
                "text": stripped,
            }
        )
    return entries


# SEM_BEGIN orchestrator_v1.prompt_composer.build_prompt_guardrail_context:v1
# type: METHOD
# use_case: Собирает checklist guardrail items из prompt chain и force-injected docs для конкретного role step.
# feature:
#   - ensure_checklist guardrail требует явный список unchecked checklist items в runtime task context
#   - Checklist source discovery идёт по prompt file, force-injected docs и embedded document references
# pre:
#   - step_config.prompt.path resolves for the provided role_dir
# post:
#   - returns guardrail_prompt_checklists only when unchecked checklist items were found
# invariant:
#   - source markdown files are read-only in this method
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - resolve_runtime_path
#   - _load_force_injected_documents
# sft: collect unchecked checklist guardrail items from the prompt and injected docs for a runtime step
# idempotent: true
# logs: query: PromptComposer build_prompt_guardrail_context
def build_prompt_guardrail_context(*, role_dir: str, step_config: PipelineStepConfig) -> dict[str, Any]:
    prompt_path = resolve_runtime_path(step_config.prompt.path, role_dir)
    if not prompt_path.exists():
        return {}

    queue: list[Path] = [prompt_path]
    queue.extend(
        path for path in _load_force_injected_documents(role_dir=role_dir) if _is_guardrail_checklist_source(path)
    )
    visited: set[Path] = set()
    ordered_sources: list[Path] = []

    while queue:
        current_path = queue.pop(0).resolve()
        if current_path in visited or not current_path.exists() or not current_path.is_file():
            continue
        visited.add(current_path)
        if not _is_guardrail_checklist_source(current_path):
            continue
        ordered_sources.append(current_path)
        current_text = current_path.read_text()
        for reference in _extract_document_references_from_text(current_text):
            resolved = _resolve_embedded_document_reference(
                reference,
                current_path=current_path,
                role_dir=role_dir,
            )
            if resolved is None or resolved in visited:
                continue
            queue.append(resolved)

    checklist_items: list[dict[str, Any]] = []
    seen_item_ids: set[str] = set()
    for source_path in ordered_sources:
        for entry in _extract_unchecked_checklist_entries(source_path):
            item_id = str(entry["id"])
            if item_id in seen_item_ids:
                continue
            seen_item_ids.add(item_id)
            checklist_items.append(entry)

    if not checklist_items:
        return {}
    return {"guardrail_prompt_checklists": checklist_items}
# SEM_END orchestrator_v1.prompt_composer.build_prompt_guardrail_context:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.load_force_injected_documents:v1
# type: METHOD
# use_case: Загружает ordered set force-injected docs для role prompt packet-а.
# feature:
#   - direct/message-role backends без filesystem discovery должны получить обязательный common packet и role metadata documents явно
# pre:
#   - role metadata for role_dir is available
# post:
#   - returns resolved document paths in deterministic order
# invariant:
#   - configured document list order is preserved except for deduplication
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - FileNotFoundError: configured mandatory document is missing
# depends:
#   - get_runtime_config
#   - load_role_metadata
# sft: resolve the ordered list of mandatory force-injected documents for a role prompt packet
# idempotent: true
# logs: -
def _load_force_injected_documents(*, role_dir: str) -> list[Path]:
    runtime = get_runtime_config()
    role_metadata = load_role_metadata(role_dir)
    configured_documents = [
        *runtime.force_injected_common_documents,
        runtime.role_metadata_path,
        *role_metadata.force_injected_documents,
    ]
    return _resolve_document_references(
        configured_documents,
        role_dir=role_dir,
        strict=True,
    )
# SEM_END orchestrator_v1.prompt_composer.load_force_injected_documents:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.render_force_injected_documents:v1
# type: METHOD
# use_case: Рендерит force-injected docs в markdown section, который встраивается в system prompt.
# feature:
#   - provider cache должен видеть стабильный, уже развернутый docs packet вместо отдельных filesystem lookups
# pre:
#   - force-injected document paths resolve successfully
# post:
#   - returns markdown string with all mandatory docs embedded
# invariant:
#   - source documents are embedded in the resolved order
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _load_force_injected_documents
# sft: render all force-injected documents into one markdown section for the stable prompt prefix
# idempotent: true
# logs: -
def _render_force_injected_documents(*, role_dir: str) -> str:
    sections = ["## Force-Injected Documents"]
    for path in _load_force_injected_documents(role_dir=role_dir):
        sections.extend(
            [
                f"### Source: `{path}`",
                path.read_text().rstrip(),
                "",
            ]
        )
    return "\n".join(sections).rstrip()
# SEM_END orchestrator_v1.prompt_composer.render_force_injected_documents:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.render_checklist_guardrail_items:v1
# type: METHOD
# use_case: Рендерит checklist guardrail items в YAML-backed prompt section для dynamic user prompt.
# feature:
#   - ensure_checklist guardrail требует, чтобы модель вернула checklist_resolutions для каждого unchecked item
# pre:
#   - task_context may or may not contain guardrail_prompt_checklists
# post:
#   - returns empty string when no checklist items exist, otherwise a checklist prompt section
# invariant:
#   - rendered checklist item order matches task_context order
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - yaml.safe_dump
# sft: render checklist guardrail items into the dynamic user prompt section
# idempotent: true
# logs: -
def _render_checklist_guardrail_items(task_context: dict[str, Any]) -> str:
    checklist_items = task_context.get("guardrail_prompt_checklists", [])
    if not isinstance(checklist_items, list) or not checklist_items:
        return ""
    rendered_items = yaml.safe_dump(checklist_items, sort_keys=False, allow_unicode=False).rstrip()
    return "\n".join(
        [
            "## Checklist Guardrail Items",
            "Return `checklist_resolutions` covering every item below.",
            f"Allowed statuses: {', '.join(_CHECKLIST_STATUS_VALUES)}.",
            "Each resolution must use exactly: `id`, `status`, `evidence`.",
            "If an item is not applicable, explain why in `evidence`. Do not use a `reason` key.",
            rendered_items,
        ]
    ).rstrip()
# SEM_END orchestrator_v1.prompt_composer.render_checklist_guardrail_items:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.render_output_contract:v1
# type: METHOD
# use_case: Формирует output contract instruction для конкретного runtime step-а.
# feature:
#   - driver parser depends on exact YAML contract wording, especially for OpenHands finish.message and checklist_resolutions
# pre:
#   - phase_id and step_config are for a known runtime step
# post:
#   - returns output contract text aligned with the step guardrails
# invariant:
#   - ensure_checklist augments the contract only when that guardrail is configured
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - PipelineStepConfig.guardrails
# sft: render the output contract instructions for a runtime step based on its phase and guardrails
# idempotent: true
# logs: -
def _render_output_contract(phase_id: PhaseId | str, step_config: PipelineStepConfig) -> str:
    finish_rule = (
        "Return no prose before or after the YAML block. "
        "If you use the OpenHands finish tool, `finish.message` must contain exactly that same YAML block."
    )
    checklist_rule = (
        " Follow the exact YAML schema and examples defined in the role prompt above."
    )
    if "ensure_checklist" in step_config.guardrails:
        checklist_rule = (
            checklist_rule
            + " If `Checklist Guardrail Items` are present, include `checklist_resolutions[]` exactly as documented in the role prompt and cover every listed item."
        )
    return " ".join([checklist_rule, finish_rule]).strip()
# SEM_END orchestrator_v1.prompt_composer.render_output_contract:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.prepare_prompt_parts:v1
# type: METHOD
# use_case: Собирает stable и dynamic части prompt-а для runtime driver-ов.
# feature:
#   - stable role prompt + force-injected docs должны отделяться от dynamic runtime context для prompt caching
#   - ensure_checklist добавляет checklist section только когда он реально нужен и доступен
# pre:
#   - configured prompt path exists
# post:
#   - returns prompt_body, force_injected_section and dynamic_section
# invariant:
#   - detailed role instructions остаются в docs, orchestration code только склеивает их
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - FileNotFoundError: pre[0] violated
# depends:
#   - build_prompt_guardrail_context
#   - _render_force_injected_documents
# sft: build stable and dynamic prompt parts for orchestrator runtime drivers
# idempotent: true
# logs: query: PromptComposer _prepare_prompt_parts
def _prepare_prompt_parts(
    *,
    phase_id: PhaseId | str,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
) -> tuple[str, str, str]:
    """Shared logic for prompt assembly.

    Returns ``(prompt_body, force_injected_section, dynamic_section)`` where:
    - *prompt_body* is the role prompt markdown (stable per role/sub_role).
    - *force_injected_section* contains force-injected methodology documents
      (stable per role).
    - *dynamic_section* contains checklist items, runtime task context and the
      output contract (changes every call).

    Drivers that support ``SystemMessage`` put ``prompt_body + force_injected``
    into the system message and ``dynamic_section`` into the user message so
    that the provider's prompt-cache covers the large stable prefix on tool-loop
    iterations 2+.
    """
    trace_id = ensure_trace_id()
    prompt_path = resolve_runtime_path(step_config.prompt.path, role_dir)

    logger.info(
        "[PromptComposer][_prepare_prompt_parts][ContextAnchor] trace_id=%s | "
        "Composing prompt parts. phase=%s, role_dir=%s, sub_role=%s, path=%s",
        trace_id,
        phase_id,
        role_dir,
        step_config.prompt.sub_role,
        prompt_path,
    )

    if not prompt_path.exists():
        logger.warning(
            "[PromptComposer][_prepare_prompt_parts][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Prompt file not found. path=%s",
            trace_id,
            prompt_path,
        )
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    prompt_body = _read_markdown(prompt_path)
    runtime_task_context = dict(task_context)
    if "ensure_checklist" in step_config.guardrails and "guardrail_prompt_checklists" not in runtime_task_context:
        runtime_task_context.update(
            build_prompt_guardrail_context(
                role_dir=role_dir,
                step_config=step_config,
            )
        )
    runtime_task_context.setdefault(
        "methodology_root_runtime",
        str(get_methodology_root_runtime()),
    )
    runtime_task_context.setdefault(
        "methodology_agents_entrypoint",
        str(resolve_methodology_entrypoint(runtime_visible=True)),
    )
    runtime_task_context.setdefault(
        "role_metadata_path",
        str(get_role_metadata_path(role_dir)),
    )

    force_injected_section = _render_force_injected_documents(role_dir=role_dir)

    dynamic_parts: list[str] = []
    checklist_section = _render_checklist_guardrail_items(runtime_task_context)
    if checklist_section:
        dynamic_parts.extend([checklist_section, ""])
    dynamic_parts.extend(
        [
            "## Runtime Task Context",
            _render_context(runtime_task_context),
            "",
            "## Output Contract",
            _render_output_contract(phase_id, step_config),
        ]
    )
    dynamic_section = "\n".join(dynamic_parts)

    return prompt_body, force_injected_section, dynamic_section
# SEM_END orchestrator_v1.prompt_composer.prepare_prompt_parts:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.compose_prompt_parts:v1
# type: METHOD
# use_case: Возвращает system/user prompt pair для backend-ов, поддерживающих message roles.
# feature:
#   - provider-side prompt caching зависит от того, что stable prefix вынесен в SystemMessage
#   - user_prompt должен содержать только dynamic runtime context, checklist section и output contract
# pre:
#   - _prepare_prompt_parts succeeds
# post:
#   - returns system_prompt and user_prompt strings ready for SystemMessage/HumanMessage
# invariant:
#   - prompt split does not drop any required runtime contract section
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _prepare_prompt_parts
# sft: compose system and user prompt messages so large stable prefixes can be cached by the provider
# idempotent: true
# logs: query: PromptComposer compose_prompt_parts
def compose_prompt_parts(
    *,
    phase_id: PhaseId | str,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
) -> tuple[str, str]:
    """Return ``(system_prompt, user_prompt)`` for drivers that support message roles.

    *system_prompt* contains the role prompt and force-injected docs (stable
    across tool-loop iterations) — placing it in ``SystemMessage`` enables
    provider-side prompt caching so subsequent API calls within the same
    tool-agent loop avoid re-processing ~20 K tokens.

    *user_prompt* contains the dynamic runtime context and output contract.
    """
    trace_id = ensure_trace_id()
    prompt_body, force_injected, dynamic = _prepare_prompt_parts(
        phase_id=phase_id,
        role_dir=role_dir,
        step_config=step_config,
        task_context=task_context,
    )
    system_prompt = prompt_body + "\n\n---\n\n" + force_injected
    user_prompt = dynamic
    logger.info(
        "[PromptComposer][compose_prompt_parts][StepComplete] trace_id=%s | "
        "Prompt parts composed. phase=%s, role_dir=%s, system_len=%d, user_len=%d",
        trace_id,
        phase_id,
        role_dir,
        len(system_prompt),
        len(user_prompt),
    )
    return system_prompt, user_prompt
# SEM_END orchestrator_v1.prompt_composer.compose_prompt_parts:v1


# SEM_BEGIN orchestrator_v1.prompt_composer.compose_prompt:v1
# type: METHOD
# use_case: Собирает полный single-string prompt для backend-ов без separate system/user message support.
# feature:
#   - OpenHands path использует один string prompt, но должен получить тот же content contract, что и direct/message-role backends
# pre:
#   - _prepare_prompt_parts succeeds
# post:
#   - returns prompt_body + force_injected docs + dynamic runtime section in one string
# invariant:
#   - prompt content matches compose_prompt_parts output semantically, differing only by transport shape
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _prepare_prompt_parts
# sft: compose the full single-string prompt for backends that do not support separate system and user messages
# idempotent: true
# logs: query: PromptComposer compose_prompt
def compose_prompt(
    *,
    phase_id: PhaseId | str,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
) -> str:
    """Compose the full prompt as a single string (for backends like OpenHands
    that do not support separate system/user messages)."""
    trace_id = ensure_trace_id()
    prompt_body, force_injected, dynamic = _prepare_prompt_parts(
        phase_id=phase_id,
        role_dir=role_dir,
        step_config=step_config,
        task_context=task_context,
    )
    prompt = prompt_body + "\n\n---\n\n" + force_injected + "\n\n" + dynamic
    logger.info(
        "[PromptComposer][compose_prompt][StepComplete] trace_id=%s | "
        "Prompt composed. phase=%s, role_dir=%s, prompt_len=%d",
        trace_id,
        phase_id,
        role_dir,
        len(prompt),
    )
    return prompt
# SEM_END orchestrator_v1.prompt_composer.compose_prompt:v1
