"""Prompt composition utilities for V1 runtime manifests."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from workflow_runtime.graph_compiler.state_schema import PhaseId, SubRole
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import resolve_runtime_path


logger = logging.getLogger(__name__)


def _read_markdown(path: Path) -> str:
    return path.read_text()


def _shared_prompt_path(sub_role: SubRole) -> Path:
    return resolve_runtime_path(f"Technical Docs/common/roles/_shared/{sub_role}_common.md")


def _render_context(task_context: dict[str, Any]) -> str:
    if not task_context:
        return "none"
    lines = []
    for key, value in task_context.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _render_output_contract(phase_id: PhaseId | str, sub_role: SubRole) -> str:
    if sub_role == SubRole.REVIEWER:
        return (
            "Return a single YAML block with keys: "
            "status, feedback, warnings."
        )
    if sub_role == SubRole.TESTER:
        return (
            "Return a single YAML block with keys: "
            "status, result, tests_passed, warnings."
        )
    if phase_id == PhaseId.COLLECT:
        return (
            "Return a single YAML block with keys: "
            "status, current_state, warnings."
        )
    if phase_id == PhaseId.PLAN:
        return (
            "Return a single YAML block with keys: "
            "status, plan, warnings. "
            "Each plan item must contain id, role, description, dependencies."
        )
    if phase_id == PhaseId.VALIDATE:
        return (
            "Return a single YAML block with keys: "
            "status, cross_cutting_result, final_result, warnings."
        )
    return (
        "Return a single YAML block with keys: "
        "status, structured_output, warnings. "
        "structured_output must contain task_id, subtask_id, role, status, "
        "changes, commands_executed, tests_passed, commits, warnings, escalation, summary."
    )


# SEM_BEGIN orchestrator_v1.prompt_composer.compose_prompt:v1
# type: METHOD
# use_case: Composes the runtime prompt for a single TaskUnit step.
# feature:
#   - Uses docs/common/roles markdown as a knowledge base
#   - Appends a phase-specific output contract for robust driver layer parsing
# pre:
#   - prompt file from step_config exists
# post:
#   - returns the final system/user prompt bundle as plain text
# invariant:
#   - markdown files are not mutated
# modifies (internal):
#   - file.docs/common/roles/*.md
# emits (external):
#   -
# errors:
#   - FileNotFoundError: pre[0] violated
# depends:
#   - resolve_runtime_path
# sft: compose runtime prompt from role markdown shared fragments and strict YAML output contract
# idempotent: true
# logs: path: docs/common/roles/*.md
def compose_prompt(
    *,
    phase_id: PhaseId | str,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
) -> str:
    trace_id = ensure_trace_id()
    prompt_path = resolve_runtime_path(step_config.prompt.path, role_dir)
    shared_path = _shared_prompt_path(step_config.prompt.sub_role)
    parts: list[str] = []

    logger.info(
        "[PromptComposer][compose_prompt][ContextAnchor] trace_id=%s | "
        "Composing prompt. phase=%s, role_dir=%s, sub_role=%s, path=%s",
        trace_id,
        phase_id,
        role_dir,
        step_config.prompt.sub_role,
        prompt_path,
    )

    # === PRE[0]: prompt file exists ===
    if not prompt_path.exists():
        logger.warning(
            "[PromptComposer][compose_prompt][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Prompt file not found. path=%s",
            trace_id,
            prompt_path,
        )
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    if shared_path.exists():
        parts.append(_read_markdown(shared_path))
    parts.append(_read_markdown(prompt_path))
    parts.append(
        "\n".join(
            [
                "## Runtime Task Context",
                _render_context(task_context),
                "",
                "## Output Contract",
                _render_output_contract(phase_id, step_config.prompt.sub_role),
            ]
        )
    )

    prompt = "\n\n---\n\n".join(parts)
    logger.info(
        "[PromptComposer][compose_prompt][StepComplete] trace_id=%s | "
        "Prompt composed. sections=%d",
        trace_id,
        len(parts),
    )
    return prompt


# SEM_END orchestrator_v1.prompt_composer.compose_prompt:v1
