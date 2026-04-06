"""Helpers for locating task artifacts and serializing structured outputs."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from workflow_runtime.graph_compiler.state_schema import StructuredOutput, SubtaskState, SubtaskStatus
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import get_runtime_config
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.tasks_storage.get_tasks_root:v1
# type: METHOD
# use_case: Resolves the configured root directory for task artifacts.
# feature:
#   - Runtime code must derive task-card paths from runtime config instead of hardcoded locations
#   - Task card 2026-03-24_1800__multi-agent-system-design, D1-D7
# pre:
#   -
# post:
#   - returns the filesystem root for task artifacts
# invariant:
#   - runtime config remains the source of truth for task storage paths
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - get_runtime_config
# sft: resolve the configured tasks root path for orchestrator task artifacts
# idempotent: true
# logs: -
def get_tasks_root() -> Path:
    runtime = get_runtime_config()
    return Path(runtime.tasks_root_default)


# SEM_END orchestrator_v1.tasks_storage.get_tasks_root:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_task_directory:v1
# type: METHOD
# use_case: Resolves the directory path for one task id under the configured tasks root.
# feature:
#   - Runtime helpers derive task and subtask artifact locations from one consistent directory layout
# pre:
#   - task_id is not empty
# post:
#   - returns the filesystem directory path for that task
# invariant:
#   - task root resolution remains config-driven
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - get_tasks_root
# sft: resolve task directory path from configured tasks root and task id
# idempotent: true
# logs: -
def resolve_task_directory(task_id: str) -> Path:
    return get_tasks_root() / task_id


# SEM_END orchestrator_v1.tasks_storage.resolve_task_directory:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_task_card:v1
# type: METHOD
# use_case: Resolves the main TASK.md path for one task id.
# feature:
#   - Runtime integrations and human review flows need a stable location for the parent task card
# pre:
#   - task_id is not empty
# post:
#   - returns the TASK.md path for the task
# invariant:
#   - task directory layout stays consistent with task-management conventions
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_task_directory
# sft: resolve the parent task card path for one task id
# idempotent: true
# logs: -
def resolve_task_card(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "TASK.md"


# SEM_END orchestrator_v1.tasks_storage.resolve_task_card:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_subtask_card:v1
# type: METHOD
# use_case: Resolves the markdown artifact path for one subtask under a task directory.
# feature:
#   - Runtime workers and reviewers address subtask artifacts by stable task/subtask identifiers
# pre:
#   - task_id and subtask_id are not empty
# post:
#   - returns the subtask markdown path
# invariant:
#   - subtask files remain colocated under the parent task directory
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_task_directory
# sft: resolve subtask markdown artifact path from task id and subtask id
# idempotent: true
# logs: -
def resolve_subtask_card(task_id: str, subtask_id: str) -> Path:
    return resolve_task_directory(task_id) / f"{subtask_id}.md"


# SEM_END orchestrator_v1.tasks_storage.resolve_subtask_card:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_openhands_conversations_directory:v1
# type: METHOD
# use_case: Resolves the directory used to persist OpenHands conversation artifacts for one task.
# feature:
#   - Worker conversation traces must live next to task memory instead of staying only in transient runtime state
#   - Task card 2026-03-24_1800__multi-agent-system-design, D9
# pre:
#   - task_id is not empty
# post:
#   - returns the conversation artifacts directory for that task
# invariant:
#   - conversation artifacts stay colocated with the parent task folder
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_task_directory
# sft: resolve per-task directory for persisted OpenHands conversation artifacts
# idempotent: true
# logs: -
def resolve_openhands_conversations_directory(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "runtime_artifacts" / "openhands_conversations"


def resolve_task_worktree_directory(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "workspace"


# SEM_END orchestrator_v1.tasks_storage.resolve_openhands_conversations_directory:v1


def _task_title(raw_text: str) -> str:
    title = " ".join(str(raw_text).strip().split())
    return title[:80] if title else "Runtime task"


def bootstrap_task_card(
    *,
    task_id: str,
    user_request: str,
    workspace_root: str,
    task_worktree_root: str,
    task_dir_path: str | None = None,
    task_card_path: str | None = None,
) -> Path:
    task_dir = Path(task_dir_path) if task_dir_path else resolve_task_directory(task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    task_card = Path(task_card_path) if task_card_path else task_dir / "TASK.md"
    if task_card.exists():
        return task_card

    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    task_card.write_text(
        "\n".join(
            [
                f"# Task {task_id}: {_task_title(user_request)}",
                "",
                "## Meta",
                "- Status: in_progress",
                "- Parent: none",
                "- Role: none",
                "- Areas: devops",
                "- Repos:",
                f"  - {workspace_root}",
                f"- Created at: {timestamp}",
                f"- Updated at: {timestamp}",
                "- Template ver 0.3",
                "",
                "## Cognitive State",
                "",
                "### Goal",
                f"- {user_request}",
                "",
                "### Task context",
                f"- Workspace: {workspace_root}",
                f"- Task worktree: {task_worktree_root}",
                "",
                "## Execution Plan",
                "- Planner will populate subtask cards after the plan phase.",
                "",
                "## StructuredOutput",
                "",
                "> Parent task card; structured output is aggregated from subtasks.",
                "",
                '<structured_output role="none">',
                "",
                "```yaml",
                f'task_id: "{task_id}"',
                'subtask_id: "parent-task"',
                'role: "none"',
                'status: "pending"',
                "changes: []",
                "commands_executed: []",
                "tests_passed: []",
                "commits: []",
                "warnings: []",
                "escalation: null",
                'summary: ""',
                "```",
                "",
                "</structured_output>",
                "",
                "## Review",
                "- Reviewer verdict: pending | pass | fail",
                "- Reviewer feedback: pending",
                "- Tester result: pending | pass | fail | skipped",
                "",
                "## Result/Answer",
                "- Pending",
                "",
                "### Evidence",
                "- Validation pending",
                "",
                "### Rollback Plan",
                "- Rollback notes pending",
                "",
                "## Details",
                "",
                "### History / Decisions / Contracts",
                "- D1 Runtime bootstrap created TASK.md",
                "",
                "### Hypotheses",
                "- H1: Planner will populate subtask cards before execute starts",
                "",
                "### Product Requirements files read and changes",
                "- Pending",
                "",
                "### Open Questions and Notes",
                "- Pending",
                "",
                "## Commits",
                "- Pending",
                "",
                "## Guides Changes",
                "- Pending",
                "",
            ]
        )
        + "\n"
    )
    return task_card


def _render_task_execution_plan(plan: list[SubtaskState]) -> str:
    if not plan:
        return "- Planner has not produced subtasks yet."
    lines: list[str] = []
    for subtask in plan:
        checkbox = "x" if subtask.status == SubtaskStatus.DONE else " "
        lines.append(f"- [{checkbox}] **[{subtask.id}](./{subtask.id}.md)** — {subtask.description}")
    return "\n".join(lines)


def _subtask_card_content(
    *,
    task_id: str,
    workspace_root: str,
    subtask: SubtaskState,
) -> str:
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    return (
        "\n".join(
            [
                f"# Task {task_id} / {subtask.id}: {_task_title(subtask.description)}",
                "",
                "## Meta",
                "- Status: planned",
                f"- Parent: {task_id}",
                f"- Role: {subtask.role}",
                "- Areas: devops",
                "- Repos:",
                f"  - {workspace_root}",
                f"- Created at: {timestamp}",
                f"- Updated at: {timestamp}",
                "- Template ver 0.3",
                "",
                "## Cognitive State",
                "",
                "### Goal",
                f"- {subtask.description}",
                "",
                "### Task context",
                f"- Parent task: {task_id}",
                f"- Workspace: {workspace_root}",
                "",
                "## Execution Plan",
                f"- [ ] {subtask.description}",
                "",
                "## StructuredOutput",
                "",
                f'<structured_output role="{subtask.role}">',
                "",
                "```yaml",
                f'task_id: "{task_id}"',
                f'subtask_id: "{subtask.id}"',
                f'role: "{subtask.role}"',
                'status: "pending"',
                "changes: []",
                "commands_executed: []",
                "tests_passed: []",
                "commits: []",
                "warnings: []",
                "escalation: null",
                'summary: ""',
                "```",
                "",
                "</structured_output>",
                "",
                "## Review",
                "- Reviewer verdict: pending | pass | fail",
                "- Reviewer feedback: pending",
                "- Tester result: pending | pass | fail | skipped",
                "",
                "## Result/Answer",
                "- Pending",
                "",
                "### Evidence",
                "- Validation pending",
                "",
                "### Rollback Plan",
                "- Rollback notes pending",
                "",
                "## Details",
                "",
                "### History / Decisions / Contracts",
                "- D1 Subtask card bootstrapped from planner output",
                "",
                "### Hypotheses",
                "- H1: Executor will update this card before finish",
                "",
                "### Product Requirements files read and changes",
                "- Pending",
                "",
                "### Open Questions and Notes",
                "- Pending",
                "",
                "## Commits",
                "- Pending",
                "",
                "## Guides Changes",
                "- Pending",
                "",
            ]
        )
        + "\n"
    )


def sync_plan_to_task_artifacts(
    *,
    task_context: dict[str, Any],
    plan: list[SubtaskState],
) -> None:
    task_id = str(task_context.get("task_id") or "").strip()
    if not task_id:
        return
    task_dir = Path(str(task_context.get("task_dir_path") or resolve_task_directory(task_id)))

    task_card = bootstrap_task_card(
        task_id=task_id,
        user_request=str(task_context.get("user_request") or task_id),
        workspace_root=str(task_context.get("source_workspace_root") or ""),
        task_worktree_root=str(task_context.get("task_worktree_root") or resolve_task_worktree_directory(task_id)),
        task_dir_path=str(task_dir),
        task_card_path=str(task_context.get("task_card_path") or ""),
    )

    task_text = task_card.read_text()
    rendered_plan = _render_task_execution_plan(plan)
    task_text = re.sub(
        r"(?ms)(## Execution Plan\n)(.*?)(\n## StructuredOutput\n)",
        lambda match: match.group(1) + rendered_plan + match.group(3),
        task_text,
        count=1,
    )
    task_text = re.sub(
        r"^- Updated at: .*$",
        f"- Updated at: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%SZ')}",
        task_text,
        count=1,
        flags=re.MULTILINE,
    )
    task_card.write_text(task_text)

    workspace_root = str(task_context.get("source_workspace_root") or "")
    for subtask in plan:
        subtask_card = task_dir / f"{subtask.id}.md"
        if subtask_card.exists():
            continue
        subtask_card.write_text(
            _subtask_card_content(
                task_id=task_id,
                workspace_root=workspace_root,
                subtask=subtask,
            )
        )


# SEM_BEGIN orchestrator_v1.tasks_storage.build_task_artifact_context:v1
# type: METHOD
# use_case: Builds stable task-artifact path hints for runtime prompts and workers.
# feature:
#   - Executors reviewers and testers need explicit filesystem paths for TASK.md subtask cards and conversation artifacts
#   - Task card 2026-03-24_1800__multi-agent-system-design, D9
# pre:
#   -
# post:
#   - returns a dict with resolved task artifact paths or an empty dict when task_id is absent
# invariant:
#   - no filesystem mutation occurs
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_task_directory
#   - resolve_task_card
#   - resolve_subtask_card
#   - resolve_openhands_conversations_directory
# sft: build resolved runtime context paths for task card subtask card and conversation artifacts
# idempotent: true
# logs: -
def build_task_artifact_context(
    task_id: str | None,
    subtask_id: str | None = None,
    *,
    task_dir_path: str | None = None,
    task_card_path: str | None = None,
    openhands_conversations_dir: str | None = None,
) -> dict[str, str]:
    if not task_id and not task_dir_path:
        return {}

    task_dir = Path(task_dir_path) if task_dir_path else resolve_task_directory(str(task_id))
    resolved_task_card = Path(task_card_path) if task_card_path else task_dir / "TASK.md"
    resolved_conversations_dir = (
        Path(openhands_conversations_dir)
        if openhands_conversations_dir
        else task_dir / "runtime_artifacts" / "openhands_conversations"
    )
    context = {
        "task_dir_path": str(task_dir),
        "task_worktree_root": str(task_dir / "workspace"),
        "task_card_path": str(resolved_task_card),
        "openhands_conversations_dir": str(resolved_conversations_dir),
    }
    if subtask_id:
        context["subtask_card_path"] = str(task_dir / f"{subtask_id}.md")
    return context


# SEM_END orchestrator_v1.tasks_storage.build_task_artifact_context:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.serialize_structured_output:v1
# type: METHOD
# use_case: Serializes a typed StructuredOutput into YAML for task artifacts and review flows.
# feature:
#   - StructuredOutput must stay portable across task cards validation and human review steps
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D7
# pre:
#   - output is a valid StructuredOutput dataclass
# post:
#   - returns YAML text with stable field order
# invariant:
#   - output object is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - yaml.YAMLError: serialization failed
# depends:
#   - asdict
#   - yaml.safe_dump
# sft: serialize structured output dataclass into YAML for task artifacts
# idempotent: true
# logs: -
def serialize_structured_output(output: StructuredOutput) -> str:
    payload = asdict(output)
    payload["status"] = str(output.status)
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)


# SEM_END orchestrator_v1.tasks_storage.serialize_structured_output:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.sync_task_cards_from_structured_output:v1
# type: METHOD
# use_case: Synchronizes task and subtask markdown cards with a successful executor StructuredOutput.
# feature:
#   - Runtime task cards stay the source of truth even when guardrails validate them immediately after executor output
#   - Checklist guardrails must inspect persisted task artifacts, not only the in-memory payload
# pre:
#   - output is a valid StructuredOutput dataclass
# post:
#   - closes subtask execution checklists and writes the latest StructuredOutput YAML into the subtask card
#   - marks the matching parent TASK.md execution-plan line as completed when it references the subtask
# invariant:
#   - only task artifact files referenced by task_context are mutated
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - OSError: referenced task artifact files could not be updated
# depends:
#   - serialize_structured_output
#   - Path.read_text
#   - Path.write_text
# sft: synchronize TASK.md and subtask markdown artifacts from executor structured output before guardrail validation
# idempotent: false
# logs: query: task artifact sync path
def sync_task_cards_from_structured_output(
    *,
    task_context: dict[str, Any],
    output: StructuredOutput,
) -> None:
    updated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    serialized_output = serialize_structured_output(output).rstrip()
    subtask_card_path = str(task_context.get("subtask_card_path") or "").strip()
    task_card_path = str(task_context.get("task_card_path") or "").strip()

    if subtask_card_path and Path(subtask_card_path).exists():
        subtask_path = Path(subtask_card_path)
        subtask_text = subtask_path.read_text()
        subtask_text = re.sub(r"^- Status: .*$", "- Status: completed", subtask_text, count=1, flags=re.MULTILINE)
        subtask_text = re.sub(
            r"^- Updated at: .*$",
            f"- Updated at: {updated_at}",
            subtask_text,
            count=1,
            flags=re.MULTILINE,
        )
        subtask_text = re.sub(
            r"(?ms)(## Execution Plan\n)(.*?)(\n## )",
            lambda match: match.group(1)
            + match.group(2).replace("- [ ]", "- [x]").replace("- [X]", "- [x]")
            + match.group(3),
            subtask_text,
            count=1,
        )
        subtask_text = re.sub(
            r'(?ms)(<structured_output role="[^"]+">\n\n```yaml\n)(.*?)(\n```\n\n</structured_output>)',
            lambda match: match.group(1) + serialized_output + match.group(3),
            subtask_text,
            count=1,
        )
        subtask_path.write_text(subtask_text)
        logger.info(
            "[TasksStorage][sync_task_cards_from_structured_output][StepComplete] trace_id=%s | "
            "Subtask card synchronized. path=%s, subtask_id=%s",
            ensure_trace_id(task_context.get("trace_id")),
            subtask_path,
            output.subtask_id,
        )

    if task_card_path and Path(task_card_path).exists():
        task_path = Path(task_card_path)
        task_text = task_path.read_text()
        task_text = re.sub(
            r"^- Updated at: .*$",
            f"- Updated at: {updated_at}",
            task_text,
            count=1,
            flags=re.MULTILINE,
        )
        task_text = re.sub(
            rf"^- \[ \](\s*\*\*\[{re.escape(output.subtask_id)}\]\(\./{re.escape(output.subtask_id)}\.md\)\*\*.*)$",
            r"- [x]\1",
            task_text,
            count=1,
            flags=re.MULTILINE,
        )
        task_path.write_text(task_text)
        logger.info(
            "[TasksStorage][sync_task_cards_from_structured_output][StepComplete] trace_id=%s | "
            "Task card synchronized. path=%s, subtask_id=%s",
            ensure_trace_id(task_context.get("trace_id")),
            task_path,
            output.subtask_id,
        )


# SEM_END orchestrator_v1.tasks_storage.sync_task_cards_from_structured_output:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.persist_openhands_conversation_artifact:v1
# type: METHOD
# use_case: Persists one OpenHands conversation transcript and metadata under the task folder.
# feature:
#   - Runtime observability and post-mortem review need durable worker conversation artifacts per task and subtask
#   - Task card 2026-03-24_1800__multi-agent-system-design, D9
# pre:
#   - task_context contains task_id or an explicit openhands_conversations_dir
#   - conversation_id is not empty
# post:
#   - writes one JSON artifact to disk and returns its path
# invariant:
#   - existing task artifacts are not overwritten because filenames include timestamp and sub-role
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - OSError: target directory or artifact file could not be written
# depends:
#   - build_task_artifact_context
#   - Path.mkdir
#   - Path.write_text
# sft: persist durable OpenHands conversation artifact JSON under the current task folder
# idempotent: false
# logs: query: task conversation artifact path
def persist_openhands_conversation_artifact(
    *,
    task_context: dict[str, Any],
    phase_id: str,
    role_dir: str,
    sub_role: str,
    conversation_id: str,
    trace_id: str | None,
    state: dict[str, Any],
    events: dict[str, Any],
    raw_text: str,
    parsed_payload: dict[str, Any],
) -> Path | None:
    resolved_trace_id = ensure_trace_id(trace_id)
    task_id = str(task_context.get("task_id") or "").strip()
    artifact_context = build_task_artifact_context(task_id, task_context.get("subtask_id"))
    task_dir_value = task_context.get("task_dir_path") or artifact_context.get("task_dir_path")
    conversation_dir_value = task_context.get("openhands_conversations_dir") or artifact_context.get(
        "openhands_conversations_dir"
    )
    if not conversation_dir_value or not conversation_id:
        logger.info(
            "[TasksStorage][persist_openhands_conversation_artifact][DecisionPoint] trace_id=%s | "
            "Branch: skip_persist. Reason: conversation_dir_configured=%s, conversation_id_present=%s",
            resolved_trace_id,
            bool(conversation_dir_value),
            bool(conversation_id),
        )
        return None
    if task_dir_value and not Path(str(task_dir_value)).exists():
        logger.info(
            "[TasksStorage][persist_openhands_conversation_artifact][DecisionPoint] trace_id=%s | "
            "Branch: skip_persist. Reason: task_dir_missing=%s",
            resolved_trace_id,
            task_dir_value,
        )
        return None

    subtask_id = str(task_context.get("subtask_id") or "phase-level")
    artifact_dir = Path(str(conversation_dir_value)) / phase_id / subtask_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    artifact_path = artifact_dir / f"{timestamp}__{sub_role}__{conversation_id}.json"

    logger.info(
        "[TasksStorage][persist_openhands_conversation_artifact][ContextAnchor] trace_id=%s | "
        "Persisting OpenHands conversation artifact. phase=%s, subtask_id=%s, sub_role=%s, path=%s",
        resolved_trace_id,
        phase_id,
        subtask_id,
        sub_role,
        artifact_path,
    )

    artifact_payload = {
        "trace_id": resolved_trace_id,
        "task_id": task_id or None,
        "subtask_id": task_context.get("subtask_id"),
        "phase_id": phase_id,
        "role_dir": role_dir,
        "sub_role": sub_role,
        "conversation_id": conversation_id,
        "task_card_path": task_context.get("task_card_path") or artifact_context.get("task_card_path"),
        "subtask_card_path": task_context.get("subtask_card_path") or artifact_context.get("subtask_card_path"),
        "saved_at": datetime.now(UTC).isoformat(),
        "state": state,
        "events": events,
        "raw_text": raw_text,
        "parsed_payload": parsed_payload,
    }
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, ensure_ascii=True))

    logger.info(
        "[TasksStorage][persist_openhands_conversation_artifact][StepComplete] trace_id=%s | "
        "OpenHands conversation artifact persisted. path=%s",
        resolved_trace_id,
        artifact_path,
    )
    return artifact_path


# SEM_END orchestrator_v1.tasks_storage.persist_openhands_conversation_artifact:v1
