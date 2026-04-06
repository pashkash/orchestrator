"""Prompt composition utilities for V1 runtime manifests.

The orchestrator's job is minimal: read the prompt file specified in
phases_and_roles.yaml, prepend the single methodology bootstrap entrypoint,
append the runtime task context (paths, IDs), and append the output
contract so the driver can parse the response.

All detailed behavioral instructions still live in the prompt markdown
files themselves. The orchestrator injects only one shared bootstrap that
points the agent to AGENTS.md instead of hardcoding a guide list.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from workflow_runtime.graph_compiler.state_schema import PhaseId, SubRole
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import (
    get_methodology_root_runtime,
    resolve_methodology_entrypoint,
    resolve_runtime_path,
)
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


def _read_markdown(path: Path) -> str:
    return path.read_text()


def _render_context(task_context: dict[str, Any]) -> str:
    if not task_context:
        return "none"
    lines = []
    for key, value in task_context.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _render_methodology_bootstrap(task_context: dict[str, Any]) -> str:
    methodology_root = str(
        task_context.get("methodology_root_runtime") or get_methodology_root_runtime()
    )
    methodology_agents_entrypoint = str(
        task_context.get("methodology_agents_entrypoint")
        or resolve_methodology_entrypoint(runtime_visible=True)
    )
    return "\n".join(
        [
            "## Methodology Bootstrap",
            f"Before any work, read `{methodology_agents_entrypoint}` and follow it as the single project methodology bootstrap.",
            "Complete the full mandatory reading chain defined by that entrypoint before you take task actions; do not stop after the first file.",
            f"Methodology root available to you: `{methodology_root}`.",
            "Do not invent fallback rules. If the entrypoint or required methodology files are missing or unreadable, stop and report BLOCKED.",
        ]
    )


def _render_output_contract(phase_id: PhaseId | str, sub_role: SubRole) -> str:
    finish_rule = (
        " Return no prose before or after the YAML block. "
        "If you use the OpenHands finish tool, `finish.message` must contain exactly that same YAML block."
    )
    if sub_role == SubRole.REVIEWER:
        return (
            "Return a single YAML block with keys: "
            "status, feedback, warnings."
            + finish_rule
        )
    if sub_role == SubRole.TESTER:
        return (
            "Return a single YAML block with keys: "
            "status, result, tests_passed, warnings."
            + finish_rule
        )
    if phase_id == PhaseId.COLLECT:
        return (
            "Return a single YAML block with keys: "
            "status, current_state, warnings."
            + finish_rule
        )
    if phase_id == PhaseId.PLAN:
        return (
            "Return a single YAML block with keys: "
            "status, plan, warnings. "
            "Each plan item must contain id, role, description, dependencies."
            + finish_rule
        )
    if phase_id == PhaseId.VALIDATE:
        return (
            "Return a single YAML block with keys: "
            "status, cross_cutting_result, final_result, warnings."
            + finish_rule
        )
    return (
        "Return a single YAML block with keys: "
        "status, structured_output, warnings. "
        "structured_output must contain task_id, subtask_id, role, status, "
        "changes, commands_executed, tests_passed, commits, warnings, escalation, summary."
        + finish_rule
    )


def compose_prompt(
    *,
    phase_id: PhaseId | str,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
) -> str:
    trace_id = ensure_trace_id()
    prompt_path = resolve_runtime_path(step_config.prompt.path, role_dir)

    logger.info(
        "[PromptComposer][compose_prompt][ContextAnchor] trace_id=%s | "
        "Composing prompt. phase=%s, role_dir=%s, sub_role=%s, path=%s",
        trace_id,
        phase_id,
        role_dir,
        step_config.prompt.sub_role,
        prompt_path,
    )

    if not prompt_path.exists():
        logger.warning(
            "[PromptComposer][compose_prompt][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Prompt file not found. path=%s",
            trace_id,
            prompt_path,
        )
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    prompt_body = _read_markdown(prompt_path)
    runtime_task_context = dict(task_context)
    runtime_task_context.setdefault(
        "methodology_root_runtime",
        str(get_methodology_root_runtime()),
    )
    runtime_task_context.setdefault(
        "methodology_agents_entrypoint",
        str(resolve_methodology_entrypoint(runtime_visible=True)),
    )

    runtime_section = "\n".join(
        [
            _render_methodology_bootstrap(runtime_task_context),
            "",
            "## Runtime Task Context",
            _render_context(runtime_task_context),
            "",
            "## Output Contract",
            _render_output_contract(phase_id, step_config.prompt.sub_role),
        ]
    )

    prompt = prompt_body + "\n\n---\n\n" + runtime_section
    logger.info(
        "[PromptComposer][compose_prompt][StepComplete] trace_id=%s | "
        "Prompt composed. phase=%s, role_dir=%s, prompt_len=%d",
        trace_id,
        phase_id,
        role_dir,
        len(prompt),
    )
    return prompt
