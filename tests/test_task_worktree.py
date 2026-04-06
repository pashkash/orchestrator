"""Tests for task-specific git worktree provisioning."""

from __future__ import annotations

import subprocess
from pathlib import Path

from workflow_runtime.integrations.task_worktree import prepare_task_methodology_docs, prepare_task_worktree


def _run(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)


def test_prepare_task_worktree_creates_git_worktree(tmp_path: Path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    _run(["git", "init", "-b", "main"], repo_root)
    _run(["git", "config", "user.email", "test@example.com"], repo_root)
    _run(["git", "config", "user.name", "Test User"], repo_root)
    (repo_root / "README.md").write_text("hello\n")
    _run(["git", "add", "README.md"], repo_root)
    _run(["git", "commit", "-m", "init"], repo_root)

    task_dir = tmp_path / "task-history" / "2026-04-05_2235__worktree-test"
    task_dir.mkdir(parents=True, exist_ok=True)

    worktree_dir = prepare_task_worktree(
        source_repo_root=str(repo_root),
        task_id="2026-04-05_2235__worktree-test",
        task_dir_path=str(task_dir),
    )

    assert worktree_dir == task_dir / "workspace"
    assert (worktree_dir / ".git").exists()
    assert (worktree_dir / "README.md").exists()

    branch = subprocess.run(
        ["git", "-C", str(worktree_dir), "branch", "--show-current"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert branch.stdout.strip() == "task/2026-04-05_2235__worktree-test"


def test_prepare_task_worktree_applies_sparse_checkout(tmp_path: Path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    _run(["git", "init", "-b", "main"], repo_root)
    _run(["git", "config", "user.email", "test@example.com"], repo_root)
    _run(["git", "config", "user.name", "Test User"], repo_root)
    (repo_root / "orchestrator").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs").mkdir(parents=True, exist_ok=True)
    (repo_root / "docker").mkdir(parents=True, exist_ok=True)
    (repo_root / "orchestrator" / "app.py").write_text("print('ok')\n")
    (repo_root / "docs" / "AGENTS.md").write_text("docs\n")
    (repo_root / "docker" / "Dockerfile").write_text("FROM scratch\n")
    _run(["git", "add", "."], repo_root)
    _run(["git", "commit", "-m", "init"], repo_root)

    task_dir = tmp_path / "task-history" / "2026-04-05_2235__sparse-test"
    task_dir.mkdir(parents=True, exist_ok=True)
    worktree_dir = prepare_task_worktree(
        source_repo_root=str(repo_root),
        task_id="2026-04-05_2235__sparse-test",
        task_dir_path=str(task_dir),
        sparse_paths=("orchestrator",),
    )

    assert (worktree_dir / "orchestrator" / "app.py").exists()
    assert not (worktree_dir / "docs").exists()
    assert not (worktree_dir / "docker").exists()


def test_prepare_task_methodology_docs_links_docs_into_task_root(tmp_path: Path):
    methodology_root = tmp_path / "source-docs"
    methodology_root.mkdir(parents=True, exist_ok=True)
    (methodology_root / "AGENTS.md").write_text("entrypoint\n")
    task_dir = tmp_path / "task-history" / "2026-04-05_2235__methodology-test"
    task_dir.mkdir(parents=True, exist_ok=True)

    docs_target = prepare_task_methodology_docs(
        task_dir_path=str(task_dir),
        methodology_source_root=str(methodology_root),
    )

    assert docs_target == task_dir / "docs"
    assert docs_target.is_symlink()
    assert docs_target.resolve() == methodology_root.resolve()
    assert (docs_target / "AGENTS.md").read_text() == "entrypoint\n"
