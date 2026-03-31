"""Prompt composition utilities for V1 runtime manifests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from workflow_runtime.graph_compiler.state_schema import PhaseId, SubRole
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import resolve_runtime_path
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.prompt_composer._read_markdown:v1
# type: METHOD
# use_case: Reads one markdown fragment from disk for prompt composition.
# feature:
#   - Runtime prompt assembly consumes shared and role-specific markdown source files
# pre:
#   - path exists
# post:
#   - returns the file contents as text
# invariant:
#   - source file is read in readonly mode
# modifies (internal):
#   - file.docs/common/roles/*.md
# emits (external):
#   -
# errors:
#   - FileNotFoundError: path does not exist
# depends:
#   - Path.read_text
# sft: read one markdown prompt fragment from disk for prompt composition
# idempotent: true
# logs: -
def _read_markdown(path: Path) -> str:
    return path.read_text()


# SEM_END orchestrator_v1.prompt_composer._read_markdown:v1


# SEM_BEGIN orchestrator_v1.prompt_composer._shared_prompt_path:v1
# type: METHOD
# use_case: Resolves the shared prompt fragment path for one sub-role.
# feature:
#   - Executor reviewer and tester can prepend shared guidance before role-specific prompt content
# pre:
#   - sub_role matches one shared prompt filename suffix
# post:
#   - returns the resolved shared prompt Path
# invariant:
#   - no filesystem mutation occurs
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_runtime_path
# sft: resolve shared markdown prompt path for one task unit sub-role
# idempotent: true
# logs: -
def _shared_prompt_path(sub_role: SubRole) -> Path:
    return resolve_runtime_path(f"Technical Docs/common/roles/_shared/{sub_role}_common.md")


# SEM_END orchestrator_v1.prompt_composer._shared_prompt_path:v1


# SEM_BEGIN orchestrator_v1.prompt_composer._render_context:v1
# type: METHOD
# use_case: Renders task context into a markdown-friendly bullet list.
# feature:
#   - Runtime prompt must expose planner and phase context in a stable human-readable shape
# pre:
#   -
# post:
#   - returns "none" for empty input or bullet-list markdown for populated context
# invariant:
#   - task_context is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: render orchestrator task context as markdown bullet list for prompt composition
# idempotent: true
# logs: -
def _render_context(task_context: dict[str, Any]) -> str:
    if not task_context:
        return "none"
    lines = []
    for key, value in task_context.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


# SEM_END orchestrator_v1.prompt_composer._render_context:v1


# SEM_BEGIN orchestrator_v1.prompt_composer._render_output_contract:v1
# type: METHOD
# use_case: Produces the strict YAML output contract text for one phase/sub-role pair.
# feature:
#   - Driver parsing stays stable because every prompt explicitly describes the expected payload keys
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4
# pre:
#   - sub_role is one of executor/reviewer/tester
# post:
#   - returns one textual YAML-contract instruction for the requested phase/sub-role
# invariant:
#   - no runtime state is mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PhaseId
#   - SubRole
# sft: derive strict YAML output contract instructions for a task unit step prompt
# idempotent: true
# logs: -
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


# SEM_END orchestrator_v1.prompt_composer._render_output_contract:v1


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
    logger.info(
        "[PromptComposer][compose_prompt][PreCheck] trace_id=%s | "
        "Checking prompt file exists. path=%s",
        trace_id,
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
