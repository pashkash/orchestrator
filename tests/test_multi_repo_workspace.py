"""Tests for YAML-driven multi-repo task workspace provisioning."""

from __future__ import annotations

from pathlib import Path
import subprocess

import run_pipeline
from workflow_runtime.agent_drivers.base_driver import DriverResult
from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineStatus, SubtaskState
from workflow_runtime.graph_compiler.yaml_manifest_parser import TaskRepositoryConfig
from workflow_runtime.integrations.task_worktree import prepare_task_workspace_repositories
from workflow_runtime.node_implementations.phases.execute_phase import run_execute_phase
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner
from tests.mocks import ScriptedDriver


def _init_repo(repo_root: Path, files: dict[str, str]) -> None:
    repo_root.mkdir(parents=True, exist_ok=True)
    for relative_path, content in files.items():
        target = repo_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    subprocess.run(["git", "init"], cwd=repo_root, check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test User",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )


def test_prepare_task_workspace_repositories_creates_sparse_worktrees(tmp_path: Path) -> None:
    devops_repo = tmp_path / "devops-source"
    backend_repo = tmp_path / "backend-source"
    _init_repo(
        devops_repo,
        {
            "orchestrator/README.md": "orchestrator",
            "docs/guide.md": "docs",
            "business-docs/spec.md": "spec",
            "ignored/skip.txt": "skip",
        },
    )
    _init_repo(
        backend_repo,
        {
            "src/App.php": "<?php",
            "config/app.php": "<?php",
            "tests/test_app.php": "<?php",
            "translations/en.php": "<?php",
            "ignored/skip.txt": "skip",
        },
    )

    task_dir = tmp_path / "task-dir"
    repo_worktrees = prepare_task_workspace_repositories(
        task_id="2026-04-06_2257__multi-repo-worktrees-langgraph-trace",
        task_dir_path=str(task_dir),
        repositories=[
            TaskRepositoryConfig(
                id="devops",
                source_repo_root=str(devops_repo),
                branch_prefix="task",
                default_sparse_paths=["orchestrator", "docs", "business-docs"],
                default_for_roles=["devops"],
            ),
            TaskRepositoryConfig(
                id="backend-prod",
                source_repo_root=str(backend_repo),
                branch_prefix="task",
                default_sparse_paths=["src", "config", "tests", "translations"],
                default_for_roles=["backend"],
            ),
        ],
    )

    assert sorted(repo_worktrees.keys()) == ["backend-prod", "devops"]
    assert (task_dir / "workspace" / "devops").is_dir()
    assert (task_dir / "workspace" / "backend-prod").is_dir()
    assert (task_dir / "workspace" / "devops" / "orchestrator" / "README.md").exists()
    assert (task_dir / "workspace" / "devops" / "docs" / "guide.md").exists()
    assert (task_dir / "workspace" / "devops" / "business-docs" / "spec.md").exists()
    assert not (task_dir / "workspace" / "devops" / "ignored").exists()
    assert (task_dir / "workspace" / "backend-prod" / "src" / "App.php").exists()
    assert (task_dir / "workspace" / "backend-prod" / "config" / "app.php").exists()
    assert (task_dir / "workspace" / "backend-prod" / "tests" / "test_app.php").exists()
    assert not (task_dir / "workspace" / "backend-prod" / "ignored").exists()


def test_workspace_override_only_reorders_primary_repository() -> None:
    repositories = run_pipeline._ordered_task_repositories(
        workspace_root="/root/dev-prod-squadder/app"
    )

    assert repositories[0].id == "backend-prod"
    assert sorted(repository.id for repository in repositories) == ["backend-prod", "devops"]


def test_execute_phase_uses_role_repo_working_directory(task_artifacts) -> None:
    captured: dict[str, str] = {}

    def capture_execute_request(request, _call_number):
        captured["working_dir"] = request.working_dir
        captured["task_workspace_repos"] = str(request.task_context["task_workspace_repos"])
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "structured_output": {
                    "task_id": "2026-03-24_1800__multi-agent-system-design",
                    "subtask_id": "backend-change",
                    "role": "backend",
                    "status": "done",
                    "changes": [],
                    "commands_executed": [],
                    "tests_passed": [],
                    "commits": [],
                    "warnings": [],
                    "escalation": None,
                    "summary": "done",
                },
                "warnings": [],
            },
            raw_text="mock-execute-pass",
        )

    runner = TaskUnitRunner(
        ScriptedDriver(
            {
                ("execute", "executor", "backend"): [capture_execute_request],
                ("execute", "reviewer", "backend"): [
                    DriverResult(
                        status=PipelineStatus.PASS,
                        payload={"status": PipelineStatus.PASS, "feedback": "ok", "warnings": []},
                        raw_text="review-ok",
                    )
                ],
                ("execute", "tester", "backend"): [
                    DriverResult(
                        status=PipelineStatus.PASS,
                        payload={"status": PipelineStatus.PASS, "result": "ok", "warnings": []},
                        raw_text="test-ok",
                    )
                ],
            }
        )
    )
    state = {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Change backend code",
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
        "trace_id": "test-role-working-dir",
        "task_dir_path": task_artifacts["task_dir_path"],
        "task_card_path": task_artifacts["task_card_path"],
        "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
        "current_state": {},
        "structured_outputs": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {"execute": 0},
        "plan": [
            SubtaskState(
                id="backend-change",
                role="backend",
                description="Modify backend code",
            )
        ],
    }

    from workflow_runtime.integrations.phase_config_loader import get_runtime_config

    result = run_execute_phase(
        state,
        task_unit_runner=runner,
        phase_config=get_runtime_config().phases[PhaseId.EXECUTE],
    )

    assert result["current_status"] == PipelineStatus.PASS
    assert captured["working_dir"] == task_artifacts["task_workspace_repos"]["backend-prod"]
