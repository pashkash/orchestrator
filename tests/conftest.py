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
    return {
        "task_dir_path": str(task_dir),
        "task_worktree_root": str(worktree),
        "task_card_path": str(task_card),
        "subtask_card_path": str(subtask_card),
        "openhands_conversations_dir": str(task_dir / "runtime_artifacts" / "openhands_conversations"),
    }
