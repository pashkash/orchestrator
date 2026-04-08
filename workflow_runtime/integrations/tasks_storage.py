"""Helpers for locating task artifacts and serializing structured outputs."""

from __future__ import annotations

import json
import hashlib
import re
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml

from workflow_runtime.graph_compiler.state_schema import (
    RuntimeArtifactRef,
    RuntimeStepRef,
    StructuredOutput,
    SubtaskState,
    SubtaskStatus,
)
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.phase_config_loader import get_runtime_config
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


def _resolve_effective_task_directory(*, task_id: str | None, task_dir_path: str | None = None) -> Path:
    if task_dir_path:
        return Path(task_dir_path)
    if task_id:
        return resolve_task_directory(task_id)
    raise ValueError("task_id or task_dir_path is required")


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


# SEM_END orchestrator_v1.tasks_storage.resolve_openhands_conversations_directory:v1


def resolve_step_payloads_directory(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "runtime_artifacts" / "step_payloads"


def resolve_cleanup_directory(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "runtime_artifacts" / "cleanup"


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_task_worktree_directory:v1
# type: METHOD
# use_case: Возвращает workspace root directory для task-local multi-repo worktrees.
# feature:
#   - run_pipeline/task_worktree helpers используют единый путь task_dir/workspace как общий контейнер worktree-ов
# pre:
#   - task_id is not empty
# post:
#   - returns the workspace directory path for that task
# invariant:
#   - worktree root stays colocated with the parent task folder
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - resolve_task_directory
# sft: resolve the workspace root directory for task-local repository worktrees
# idempotent: true
# logs: -
def resolve_task_worktree_directory(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "workspace"
# SEM_END orchestrator_v1.tasks_storage.resolve_task_worktree_directory:v1


def _task_title(raw_text: str) -> str:
    title = " ".join(str(raw_text).strip().split())
    return title[:80] if title else "Runtime task"


def _render_repos_lines(
    *,
    workspace_root: str,
    workspace_roots: dict[str, str] | None = None,
) -> list[str]:
    rendered_roots = [
        str(path).strip()
        for path in (workspace_roots or {}).values()
        if str(path).strip()
    ]
    if not rendered_roots and workspace_root.strip():
        rendered_roots = [workspace_root.strip()]
    return [f"  - {path}" for path in rendered_roots]


# SEM_BEGIN orchestrator_v1.tasks_storage.bootstrap_task_card:v1
# type: METHOD
# use_case: Создаёт parent TASK.md по шаблону task framework, если он ещё не существует.
# feature:
#   - Каждый orchestrator run должен иметь task card как рабочую память и точку синхронизации для planner/execute
# pre:
#   - task_id is not empty
# post:
#   - returns a task card path that exists on disk
# invariant:
#   - existing TASK.md is reused unchanged
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - resolve_task_directory
# sft: create the parent task card from the standard template if it does not already exist
# idempotent: false
# logs: -
def bootstrap_task_card(
    *,
    task_id: str,
    user_request: str,
    workspace_root: str,
    task_worktree_root: str,
    workspace_roots: dict[str, str] | None = None,
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
                *_render_repos_lines(workspace_root=workspace_root, workspace_roots=workspace_roots),
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
# SEM_END orchestrator_v1.tasks_storage.bootstrap_task_card:v1


def _render_task_execution_plan(plan: list[SubtaskState]) -> str:
    if not plan:
        return "- Planner has not produced subtasks yet."
    lines: list[str] = []
    for subtask in plan:
        checkbox = "x" if subtask.status == SubtaskStatus.DONE else " "
        lines.append(f"- [{checkbox}] **[{subtask.id}](./{subtask.id}.md)** — {subtask.description}")
    return "\n".join(lines)


# SEM_BEGIN orchestrator_v1.tasks_storage.subtask_card_content:v1
# type: METHOD
# use_case: Рендерит markdown content для нового subtask card из planner output.
# feature:
#   - Каждый planned subtask должен получить self-contained card с тем же task framework contract, что и parent TASK.md
# pre:
#   - subtask contains id, role and description
# post:
#   - returns markdown content for one subtask card
# invariant:
#   - generated subtask card follows the shared task framework layout
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _task_title
#   - _render_repos_lines
# sft: render the markdown body for one subtask card using the planner output and workspace metadata
# idempotent: true
# logs: -
def _subtask_card_content(
    *,
    task_id: str,
    workspace_root: str,
    workspace_roots: dict[str, str] | None,
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
                *_render_repos_lines(workspace_root=workspace_root, workspace_roots=workspace_roots),
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
# SEM_END orchestrator_v1.tasks_storage.subtask_card_content:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.sync_plan_to_task_artifacts:v1
# type: METHOD
# use_case: Синхронизирует planner output в parent TASK.md и создаёт subtask cards.
# feature:
#   - Planner plan должен материализоваться в task-history markdown artifacts до execute phase
# pre:
#   - task_context contains task_id
# post:
#   - parent TASK.md execution plan updated
#   - missing subtask markdown files created for every planned subtask
# invariant:
#   - existing subtask cards are not overwritten
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - bootstrap_task_card
# sft: update the parent task card execution plan and create missing subtask cards from planner output
# idempotent: false
# logs: -
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
        workspace_roots=dict(task_context.get("source_workspace_roots") or {}),
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
    workspace_roots = dict(task_context.get("source_workspace_roots") or {})
    for subtask in plan:
        subtask_card = task_dir / f"{subtask.id}.md"
        if subtask_card.exists():
            continue
        subtask_card.write_text(
            _subtask_card_content(
                task_id=task_id,
                workspace_root=workspace_root,
                workspace_roots=workspace_roots,
                subtask=subtask,
            )
        )
# SEM_END orchestrator_v1.tasks_storage.sync_plan_to_task_artifacts:v1


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
#   - phase-level contexts expose TASK.md only when the parent card already exists
#   - subtask-level contexts always expose the parent TASK.md path together with the subtask card path
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
        "openhands_conversations_dir": str(resolved_conversations_dir),
    }
    include_parent_task_card = bool(subtask_id) or resolved_task_card.exists()
    if include_parent_task_card:
        context["task_card_path"] = str(resolved_task_card)
    if resolved_task_card.exists():
        context["task_card_content"] = resolved_task_card.read_text()
    if subtask_id:
        subtask_card = task_dir / f"{subtask_id}.md"
        context["subtask_card_path"] = str(subtask_card)
        if subtask_card.exists():
            context["subtask_card_content"] = subtask_card.read_text()
    return context


# SEM_END orchestrator_v1.tasks_storage.build_task_artifact_context:v1


def _normalize_runtime_json(value: Any) -> Any:
    if is_dataclass(value):
        return _normalize_runtime_json(asdict(value))
    if hasattr(value, "value"):
        return getattr(value, "value")
    if isinstance(value, dict):
        return {str(key): _normalize_runtime_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_runtime_json(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_runtime_json(item) for item in value]
    return value


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_normalize_runtime_json(payload), indent=2, ensure_ascii=True))


def _sha256_for_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_runtime_subtask_id(subtask_id: str | None) -> str:
    normalized = str(subtask_id or "").strip()
    return normalized or "phase-level"


def build_runtime_step_key(phase_id: str, subtask_id: str | None, sub_role: str) -> str:
    return f"{phase_id}/{_normalize_runtime_subtask_id(subtask_id)}/{sub_role}"


def resolve_step_attempt_directory(
    *,
    task_id: str,
    phase_id: str,
    subtask_id: str | None,
    sub_role: str,
    attempt: int,
    task_dir_path: str | None = None,
) -> Path:
    return (
        _resolve_effective_task_directory(task_id=task_id, task_dir_path=task_dir_path)
        / "runtime_artifacts"
        / "step_payloads"
        / phase_id
        / _normalize_runtime_subtask_id(subtask_id)
        / sub_role
        / f"attempt-{attempt:03d}"
    )


def _build_runtime_artifact_ref(
    *,
    artifact_kind: str,
    path: Path,
    phase_id: str,
    subtask_id: str | None,
    sub_role: str,
    attempt: int,
    created_at: str,
    trace_id: str,
) -> RuntimeArtifactRef:
    return {
        "artifact_kind": artifact_kind,
        "path": str(path),
        "phase_id": phase_id,
        "subtask_id": _normalize_runtime_subtask_id(subtask_id),
        "sub_role": sub_role,
        "attempt": attempt,
        "created_at": created_at,
        "trace_id": trace_id,
        "sha256": _sha256_for_path(path),
    }


def _load_runtime_artifact_refs(path: Path) -> list[RuntimeArtifactRef]:
    if not path.exists():
        return []
    loaded = json.loads(path.read_text())
    return list(loaded) if isinstance(loaded, list) else []


def _persist_step_summary(
    *,
    summary_path: Path,
    refs_path: Path,
    payload: dict[str, Any],
) -> None:
    refs = _load_runtime_artifact_refs(refs_path)
    summary_payload = {
        **payload,
        "artifact_refs_path": str(refs_path),
        "artifact_refs": refs,
    }
    _write_json_file(summary_path, summary_payload)


def persist_driver_step_artifacts(
    *,
    task_context: dict[str, Any],
    phase_id: str,
    role_dir: str,
    sub_role: str,
    attempt: int,
    trace_id: str | None,
    status: str,
    request_artifact: dict[str, Any],
    raw_text: str,
    parsed_payload: dict[str, Any],
    artifact_refs: dict[str, Any] | None = None,
) -> RuntimeStepRef | None:
    resolved_trace_id = ensure_trace_id(trace_id)
    task_id = str(task_context.get("task_id") or "").strip()
    task_dir_path = str(task_context.get("task_dir_path") or "").strip()
    if not task_id and not task_dir_path:
        return None
    effective_task_id = task_id or Path(task_dir_path).name
    subtask_id = task_context.get("subtask_id")
    attempt_dir = resolve_step_attempt_directory(
        task_id=effective_task_id,
        phase_id=phase_id,
        subtask_id=subtask_id,
        sub_role=sub_role,
        attempt=attempt,
        task_dir_path=task_dir_path or None,
    )
    attempt_dir.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(UTC).isoformat()

    driver_request_path = attempt_dir / "driver_request.json"
    prompt_path = attempt_dir / "prompt.txt"
    raw_text_path = attempt_dir / "raw_text.md"
    parsed_payload_path = attempt_dir / "parsed_payload.json"
    refs_path = attempt_dir / "artifact_refs.json"
    summary_path = attempt_dir / "step_summary.json"

    _write_json_file(driver_request_path, request_artifact)
    prompt_path.write_text(str(request_artifact.get("full_prompt") or ""))
    raw_text_path.write_text(raw_text)
    _write_json_file(parsed_payload_path, parsed_payload)

    persisted_refs: list[RuntimeArtifactRef] = [
        _build_runtime_artifact_ref(
            artifact_kind="driver_request",
            path=driver_request_path,
            phase_id=phase_id,
            subtask_id=subtask_id,
            sub_role=sub_role,
            attempt=attempt,
            created_at=created_at,
            trace_id=resolved_trace_id,
        ),
        _build_runtime_artifact_ref(
            artifact_kind="prompt",
            path=prompt_path,
            phase_id=phase_id,
            subtask_id=subtask_id,
            sub_role=sub_role,
            attempt=attempt,
            created_at=created_at,
            trace_id=resolved_trace_id,
        ),
        _build_runtime_artifact_ref(
            artifact_kind="raw_text",
            path=raw_text_path,
            phase_id=phase_id,
            subtask_id=subtask_id,
            sub_role=sub_role,
            attempt=attempt,
            created_at=created_at,
            trace_id=resolved_trace_id,
        ),
        _build_runtime_artifact_ref(
            artifact_kind="parsed_payload",
            path=parsed_payload_path,
            phase_id=phase_id,
            subtask_id=subtask_id,
            sub_role=sub_role,
            attempt=attempt,
            created_at=created_at,
            trace_id=resolved_trace_id,
        ),
    ]
    for artifact_kind, raw_path in dict(artifact_refs or {}).items():
        candidate_path = Path(str(raw_path))
        if not candidate_path.exists():
            continue
        persisted_refs.append(
            _build_runtime_artifact_ref(
                artifact_kind=str(artifact_kind),
                path=candidate_path,
                phase_id=phase_id,
                subtask_id=subtask_id,
                sub_role=sub_role,
                attempt=attempt,
                created_at=created_at,
                trace_id=resolved_trace_id,
            )
        )
    _write_json_file(refs_path, persisted_refs)

    step_ref: RuntimeStepRef = {
        "step_key": build_runtime_step_key(phase_id, subtask_id, sub_role),
        "phase_id": phase_id,
        "subtask_id": _normalize_runtime_subtask_id(subtask_id),
        "sub_role": sub_role,
        "attempt": attempt,
        "status": str(status),
        "summary_path": str(summary_path),
        "artifact_refs": persisted_refs,
    }
    _persist_step_summary(
        summary_path=summary_path,
        refs_path=refs_path,
        payload={
            "trace_id": resolved_trace_id,
            "task_id": effective_task_id,
            "phase_id": phase_id,
            "subtask_id": _normalize_runtime_subtask_id(subtask_id),
            "role_dir": role_dir,
            "sub_role": sub_role,
            "attempt": attempt,
            "status": str(status),
            "saved_at": created_at,
        },
    )
    return step_ref


def persist_guardrail_artifacts(
    *,
    step_ref: RuntimeStepRef | None,
    trace_id: str | None,
    guardrail_payload: dict[str, Any],
    route_decision: str,
    feedback: str,
) -> RuntimeArtifactRef | None:
    if step_ref is None:
        return None
    summary_path = Path(str(step_ref["summary_path"]))
    attempt_dir = summary_path.parent
    refs_path = attempt_dir / "artifact_refs.json"
    guardrail_path = attempt_dir / "guardrail_result.json"
    created_at = datetime.now(UTC).isoformat()
    _write_json_file(
        guardrail_path,
        {
            **guardrail_payload,
            "route_decision": route_decision,
            "feedback": feedback,
            "saved_at": created_at,
        },
    )
    ref = _build_runtime_artifact_ref(
        artifact_kind="guardrail_result",
        path=guardrail_path,
        phase_id=str(step_ref["phase_id"]),
        subtask_id=str(step_ref.get("subtask_id") or ""),
        sub_role=str(step_ref["sub_role"]),
        attempt=int(step_ref["attempt"]),
        created_at=created_at,
        trace_id=ensure_trace_id(trace_id),
    )
    refs = [*_load_runtime_artifact_refs(refs_path), ref]
    _write_json_file(refs_path, refs)
    summary_payload = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    summary_payload.update(
        {
            "guardrail_status": str(guardrail_payload.get("status") or ""),
            "guardrail_warnings": list(guardrail_payload.get("warnings") or []),
            "route_decision": route_decision,
            "latest_guardrail_feedback": feedback,
        }
    )
    _persist_step_summary(summary_path=summary_path, refs_path=refs_path, payload=summary_payload)
    return ref


def persist_task_unit_result_artifact(
    *,
    step_ref: RuntimeStepRef | None,
    trace_id: str | None,
    task_unit_result: Any,
) -> RuntimeArtifactRef | None:
    if step_ref is None:
        return None
    summary_path = Path(str(step_ref["summary_path"]))
    attempt_dir = summary_path.parent
    refs_path = attempt_dir / "artifact_refs.json"
    result_path = attempt_dir / "task_unit_result.json"
    created_at = datetime.now(UTC).isoformat()
    _write_json_file(result_path, task_unit_result)
    ref = _build_runtime_artifact_ref(
        artifact_kind="task_unit_result",
        path=result_path,
        phase_id=str(step_ref["phase_id"]),
        subtask_id=str(step_ref.get("subtask_id") or ""),
        sub_role=str(step_ref["sub_role"]),
        attempt=int(step_ref["attempt"]),
        created_at=created_at,
        trace_id=ensure_trace_id(trace_id),
    )
    refs = [*_load_runtime_artifact_refs(refs_path), ref]
    _write_json_file(refs_path, refs)
    summary_payload = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    summary_payload.update(
        {
            "task_unit_result_status": str(getattr(task_unit_result, "status", "")),
            "task_unit_warnings": list(getattr(task_unit_result, "warnings", []) or []),
            "completed_at": created_at,
        }
    )
    _persist_step_summary(summary_path=summary_path, refs_path=refs_path, payload=summary_payload)
    return ref


def persist_human_gate_artifact(
    *,
    task_context: dict[str, Any],
    phase_id: str,
    subtask_id: str | None,
    attempt: int,
    trace_id: str | None,
    artifact_kind: str,
    payload: dict[str, Any],
    summary_path: str | None = None,
) -> RuntimeArtifactRef | None:
    resolved_trace_id = ensure_trace_id(trace_id)
    created_at = datetime.now(UTC).isoformat()
    target_summary_path = Path(summary_path) if summary_path else None
    if target_summary_path is not None:
        attempt_dir = target_summary_path.parent
    else:
        task_id = str(task_context.get("task_id") or "").strip()
        task_dir_path = str(task_context.get("task_dir_path") or "").strip()
        if not task_id and not task_dir_path:
            return None
        effective_task_id = task_id or Path(task_dir_path).name
        attempt_dir = resolve_step_attempt_directory(
            task_id=effective_task_id,
            phase_id=phase_id,
            subtask_id=subtask_id,
            sub_role="human_gate",
            attempt=attempt,
            task_dir_path=task_dir_path or None,
        )
    attempt_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = attempt_dir / f"{artifact_kind}.json"
    _write_json_file(artifact_path, {**payload, "saved_at": created_at})
    ref = _build_runtime_artifact_ref(
        artifact_kind=artifact_kind,
        path=artifact_path,
        phase_id=phase_id,
        subtask_id=subtask_id,
        sub_role="human_gate",
        attempt=attempt,
        created_at=created_at,
        trace_id=resolved_trace_id,
    )
    refs_path = attempt_dir / "artifact_refs.json"
    summary_file = attempt_dir / "step_summary.json"
    if refs_path.exists():
        refs = [*_load_runtime_artifact_refs(refs_path), ref]
        _write_json_file(refs_path, refs)
        if summary_file.exists():
            summary_payload = json.loads(summary_file.read_text())
            if isinstance(summary_payload, dict):
                _persist_step_summary(summary_path=summary_file, refs_path=refs_path, payload=summary_payload)
    return ref


def persist_cleanup_manifest(
    *,
    state: dict[str, Any],
    trace_id: str | None,
) -> RuntimeArtifactRef | None:
    task_id = str(state.get("task_id") or "").strip()
    task_dir_path = str(state.get("task_dir_path") or "").strip()
    if not task_id and not task_dir_path:
        return None
    effective_task_id = task_id or Path(task_dir_path).name
    cleanup_dir = _resolve_effective_task_directory(task_id=effective_task_id, task_dir_path=task_dir_path) / "runtime_artifacts" / "cleanup"
    cleanup_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = cleanup_dir / "cleanup_manifest.json"
    created_at = datetime.now(UTC).isoformat()
    payload = {
        "task_id": task_id,
        "trace_id": ensure_trace_id(trace_id),
        "saved_at": created_at,
        "task_dir_path": state.get("task_dir_path"),
        "task_worktree_root": state.get("task_worktree_root"),
        "task_workspace_repos": state.get("task_workspace_repos", {}),
        "methodology_root_runtime": state.get("methodology_root_runtime"),
        "cleanup_requires_explicit_user_approval": True,
    }
    _write_json_file(manifest_path, payload)
    return _build_runtime_artifact_ref(
        artifact_kind="cleanup_manifest",
        path=manifest_path,
        phase_id=str(state.get("current_phase") or "validate"),
        subtask_id=None,
        sub_role="cleanup",
        attempt=1,
        created_at=created_at,
        trace_id=ensure_trace_id(trace_id),
    )


def read_runtime_step_summary(path: str) -> dict[str, Any]:
    loaded = json.loads(Path(path).read_text())
    return loaded if isinstance(loaded, dict) else {}


# SEM_BEGIN orchestrator_v1.tasks_storage.apply_task_artifact_writes:v1
# type: METHOD
# use_case: Applies runtime-controlled full-file task artifact writes returned by a driver payload.
# feature:
#   - Planner and supervisor can update TASK.md or subtask cards without OpenHands by returning full replacement content
#   - Runtime keeps filesystem mutation deterministic and bounded to declared task artifacts
# pre:
#   - payload may contain task_artifact_writes list
# post:
#   - writes only allowed task artifact files and returns validation warnings for skipped items
# invariant:
#   - only task_card_path and subtask_card_path from task_context are writable through this helper
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   -
# depends:
#   - Path.write_text
# sft: apply runtime-controlled full file task artifact writes from a driver payload while validating target paths
# idempotent: false
# logs: query: task artifact write path
def apply_task_artifact_writes(
    *,
    task_context: dict[str, Any],
    payload: dict[str, Any],
) -> list[str]:
    raw_writes = payload.get("task_artifact_writes")
    if not isinstance(raw_writes, list):
        return []

    warnings: list[str] = []
    allowed_paths = {
        str(Path(path_value).resolve())
        for path_value in (
            task_context.get("task_card_path"),
            task_context.get("subtask_card_path"),
        )
        if path_value
    }
    trace_id = ensure_trace_id(task_context.get("trace_id"))

    for index, raw_write in enumerate(raw_writes, start=1):
        if not isinstance(raw_write, dict):
            warnings.append(f"task_artifact_writes[{index}] must be a mapping")
            continue
        target_path = str(raw_write.get("path") or "").strip()
        mode = str(raw_write.get("mode") or "").strip().lower()
        content = raw_write.get("content")
        if not target_path or not isinstance(content, str):
            warnings.append(f"task_artifact_writes[{index}] must contain string path and content")
            continue
        resolved_target = str(Path(target_path).resolve())
        if resolved_target not in allowed_paths:
            warnings.append(f"task_artifact_writes[{index}] targets a non-task artifact path: {target_path}")
            continue
        if mode != "full_replace":
            warnings.append(f"task_artifact_writes[{index}] uses unsupported mode: {mode or '<empty>'}")
            continue
        target = Path(resolved_target)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        logger.info(
            "[TasksStorage][apply_task_artifact_writes][StepComplete] trace_id=%s | "
            "Task artifact written. path=%s, mode=%s",
            trace_id,
            target,
            mode,
        )
    return warnings


# SEM_END orchestrator_v1.tasks_storage.apply_task_artifact_writes:v1


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
#   - existing task artifacts are not overwritten because filenames include timestamp, sub-role, and a unique suffix
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
    artifact_nonce = uuid4().hex[:8]
    artifact_path = artifact_dir / f"{timestamp}__{sub_role}__{conversation_id}__{artifact_nonce}.json"

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
