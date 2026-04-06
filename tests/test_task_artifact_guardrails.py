"""Tests for runtime task-artifact guardrails and storage helpers."""

from __future__ import annotations

import json
from pathlib import Path

from workflow_runtime.graph_compiler.state_schema import (
    FileChange,
    PipelineStatus,
    StructuredOutput,
    StructuredOutputStatus,
    SubtaskState,
    SubRole,
)
from workflow_runtime.integrations.tasks_storage import (
    bootstrap_task_card,
    build_task_artifact_context,
    persist_openhands_conversation_artifact,
    sync_plan_to_task_artifacts,
    sync_task_cards_from_structured_output,
)
from workflow_runtime.node_implementations.task_unit.guardrail_checker import run_guardrails


def test_ensure_checklist_passes_when_task_and_subtask_are_closed(task_artifacts):
    result = run_guardrails(
        phase_id="execute",
        step_name=SubRole.EXECUTOR,
        payload={
            "status": "PASS",
            "structured_output": {
                "task_id": "task-1",
                "subtask_id": "subtask-1",
                "role": "devops",
                "status": "done",
                "changes": [],
                "commands_executed": [],
                "tests_passed": [],
                "commits": [],
                "warnings": [],
                "summary": "ok",
            },
        },
        guardrails=["ensure_structured_output", "ensure_checklist"],
        task_context=task_artifacts,
        trace_id="test-checklist-pass",
    )

    assert result.status == PipelineStatus.PASS
    assert result.warnings == []


def test_ensure_checklist_fails_when_subtask_has_open_checkbox(task_artifacts):
    subtask_path = Path(task_artifacts["subtask_card_path"])
    subtask_path.write_text(
        "\n".join(
            [
                "# Subtask",
                "",
                "## Execution Plan",
                "- [ ] Implement runtime change",
            ]
        )
    )

    result = run_guardrails(
        phase_id="execute",
        step_name=SubRole.EXECUTOR,
        payload={"status": "PASS", "structured_output": {}},
        guardrails=["ensure_checklist"],
        task_context=task_artifacts,
        trace_id="test-checklist-fail",
    )

    assert result.status == PipelineStatus.NEEDS_FIX_EXECUTOR
    assert any("Unchecked checklist items remain" in warning for warning in result.warnings)


def test_sync_task_cards_from_structured_output_closes_checklists(task_artifacts):
    Path(task_artifacts["task_card_path"]).write_text(
        "\n".join(
            [
                "# Task",
                "",
                "## Execution Plan",
                "- [ ] **[devops-update-runtime-config](./devops-update-runtime-config.md)** — update runtime config",
            ]
        )
    )
    sync_task_cards_from_structured_output(
        task_context={**task_artifacts, "task_id": "task-1", "trace_id": "test-sync-cards"},
        output=StructuredOutput(
            task_id="task-1",
            subtask_id="devops-update-runtime-config",
            role="devops",
            status=StructuredOutputStatus.DONE,
            changes=[
                FileChange(
                    file="orchestrator/config/flow.yaml",
                    type="modified",
                    description="Updated runtime flow manifest",
                )
            ],
            commands_executed=["uv run pytest tests/ -v"],
            tests_passed=["test_happy_path"],
            commits=[],
            warnings=[],
            escalation=None,
            summary="Synchronized task artifacts",
        ),
    )

    result = run_guardrails(
        phase_id="execute",
        step_name=SubRole.EXECUTOR,
        payload={
            "status": "PASS",
            "structured_output": {
                "task_id": "task-1",
                "subtask_id": "devops-update-runtime-config",
                "role": "devops",
                "status": "done",
                "changes": [],
                "commands_executed": [],
                "tests_passed": [],
                "commits": [],
                "warnings": [],
                "summary": "ok",
            },
        },
        guardrails=["ensure_structured_output", "ensure_checklist"],
        task_context=task_artifacts,
        trace_id="test-checklist-pass-after-sync",
    )

    assert result.status == PipelineStatus.PASS
    assert "- [x] Implement runtime change" in Path(task_artifacts["subtask_card_path"]).read_text()
    assert (
        "- [x] **[devops-update-runtime-config](./devops-update-runtime-config.md)**"
        in Path(task_artifacts["task_card_path"]).read_text()
    )


def test_build_task_artifact_context_returns_expected_paths():
    context = build_task_artifact_context(
        "2026-03-24_1800__multi-agent-system-design",
        "devops-update-runtime-config",
    )

    assert context["task_worktree_root"].endswith("/2026-03-24_1800__multi-agent-system-design/workspace")
    assert context["task_card_path"].endswith("/2026-03-24_1800__multi-agent-system-design/TASK.md")
    assert context["subtask_card_path"].endswith(
        "/2026-03-24_1800__multi-agent-system-design/devops-update-runtime-config.md"
    )
    assert context["openhands_conversations_dir"].endswith(
        "/2026-03-24_1800__multi-agent-system-design/runtime_artifacts/openhands_conversations"
    )


def test_bootstrap_task_card_creates_runtime_task_card(tmp_path: Path):
    task_card = bootstrap_task_card(
        task_id="2026-03-24_1800__multi-agent-system-design",
        user_request="Create a runtime task card automatically",
        workspace_root="/root/squadder-devops",
        task_worktree_root=str(tmp_path / "workspace"),
        task_dir_path=str(tmp_path),
        task_card_path=str(tmp_path / "TASK.md"),
    )

    assert task_card.exists()
    contents = task_card.read_text()
    assert "## Execution Plan" in contents
    assert "Planner will populate subtask cards after the plan phase." in contents
    assert 'task_id: "2026-03-24_1800__multi-agent-system-design"' in contents


def test_sync_plan_to_task_artifacts_creates_subtask_cards_and_updates_task_plan(tmp_path: Path):
    task_dir = tmp_path / "2026-03-24_1800__multi-agent-system-design"
    task_dir.mkdir(parents=True, exist_ok=True)
    task_card_path = task_dir / "TASK.md"
    bootstrap_task_card(
        task_id="2026-03-24_1800__multi-agent-system-design",
        user_request="Create runtime artifacts from planner output",
        workspace_root="/root/squadder-devops",
        task_worktree_root=str(task_dir / "workspace"),
        task_dir_path=str(task_dir),
        task_card_path=str(task_card_path),
    )

    sync_plan_to_task_artifacts(
        task_context={
            "task_id": "2026-03-24_1800__multi-agent-system-design",
            "user_request": "Create runtime artifacts from planner output",
            "source_workspace_root": "/root/squadder-devops",
            "task_worktree_root": str(task_dir / "workspace"),
            "task_dir_path": str(task_dir),
            "task_card_path": str(task_card_path),
        },
        plan=[
            SubtaskState(
                id="devops-update-runtime-config",
                role="devops",
                description="Update runtime config",
            )
        ],
    )

    task_contents = task_card_path.read_text()
    subtask_card = task_dir / "devops-update-runtime-config.md"
    assert "- [ ] **[devops-update-runtime-config](./devops-update-runtime-config.md)** — Update runtime config" in task_contents
    assert subtask_card.exists()
    assert "- [ ] Update runtime config" in subtask_card.read_text()
    assert '<structured_output role="devops">' in subtask_card.read_text()


def test_persist_openhands_conversation_artifact_writes_json(task_artifacts):
    artifact_path = persist_openhands_conversation_artifact(
        task_context={**task_artifacts, "task_id": "2026-03-24_1800__multi-agent-system-design", "subtask_id": "devops-update-runtime-config"},
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        conversation_id="conv-123",
        trace_id="test-conversation-persist",
        state={"execution_status": "FINISHED"},
        events={"events": [{"id": "evt-1"}]},
        raw_text="```yaml\nstatus: PASS\n```",
        parsed_payload={"status": "PASS"},
    )

    assert artifact_path is not None
    assert artifact_path.exists()
    saved = json.loads(artifact_path.read_text())
    assert saved["conversation_id"] == "conv-123"
    assert saved["phase_id"] == "execute"
    assert saved["sub_role"] == "executor"
