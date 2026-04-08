"""Shared pytest fixtures for the V1 orchestrator."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def initial_state(task_artifacts) -> dict:
    return {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Refactor orchestrator to the V1 phase-driven architecture",
        "workspace_root": "/root/squadder-devops",
        "task_worktree_root": task_artifacts["task_worktree_root"],
        "primary_workspace_repo_id": "devops",
        "source_workspace_roots": {
            "devops": "/root/squadder-devops",
            "backend-prod": "/root/dev-prod-squadder/app",
        },
        "role_workspace_repo_map": {
            "devops": "devops",
            "architect": "devops",
            "backend": "backend-prod",
        },
        "task_workspace_repos": task_artifacts["task_workspace_repos"],
        "trace_id": "test-trace-id",
        "task_dir_path": task_artifacts["task_dir_path"],
        "task_card_path": task_artifacts["task_card_path"],
        "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
        "current_state": {},
        "plan": [],
        "structured_outputs": [],
        "human_decisions": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {},
        "commits": [],
        "runtime_step_refs": [],
        "latest_step_ref_by_key": {},
        "pending_approval_ref": None,
        "human_decision_refs": [],
        "cleanup_manifest_ref": None,
    }


@pytest.fixture
def task_artifacts(tmp_path: Path) -> dict[str, str]:
    task_dir = tmp_path / "2026-03-24_1800__multi-agent-system-design"
    task_dir.mkdir(parents=True, exist_ok=True)
    task_card = task_dir / "TASK.md"
    subtask_card = task_dir / "devops-update-runtime-config.md"
    task_card.write_text(
        "\n".join(
            [
                "# Task",
                "",
                "## Execution Plan",
                "- [x] Historical item",
            ]
        )
    )
    subtask_card.write_text(
        "\n".join(
            [
                "# Subtask",
                "",
                "## Execution Plan",
                "- [x] Implement runtime change",
            ]
        )
    )
    worktree = task_dir / "workspace"
    worktree.mkdir(parents=True, exist_ok=True)
    (worktree / "devops").mkdir(parents=True, exist_ok=True)
    (worktree / "backend-prod").mkdir(parents=True, exist_ok=True)
    return {
        "task_dir_path": str(task_dir),
        "task_worktree_root": str(worktree),
        "task_workspace_repos": {
            "devops": str(worktree / "devops"),
            "backend-prod": str(worktree / "backend-prod"),
        },
        "task_card_path": str(task_card),
        "subtask_card_path": str(subtask_card),
        "openhands_conversations_dir": str(task_dir / "runtime_artifacts" / "openhands_conversations"),
    }
