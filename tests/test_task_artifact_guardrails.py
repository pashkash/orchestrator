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
    apply_task_artifact_writes,
    bootstrap_task_card,
    build_task_artifact_context,
    persist_cleanup_manifest,
    persist_driver_step_artifacts,
    read_runtime_step_summary,
    persist_openhands_conversation_artifact,
    sync_plan_to_task_artifacts,
    sync_task_cards_from_structured_output,
)
from workflow_runtime.integrations.phase_config_loader import get_runtime_config
from workflow_runtime.integrations.prompt_composer import build_prompt_guardrail_context
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


def test_build_prompt_guardrail_context_collects_role_common_and_standards_items():
    runtime = get_runtime_config()

    context = build_prompt_guardrail_context(
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.reviewer,
    )

    checklist_items = context["guardrail_prompt_checklists"]
    sources = {item["source"] for item in checklist_items}

    assert "common/roles/devops/reviewer.md" in sources
    assert "common/roles/_shared/reviewer_common.md" in sources
    assert "common/standards/code_semantic_markup.md" in sources
    assert "common/standards/ai_friendly_logging_markup.md" in sources
    assert "common/templates/task_template.md" not in sources


def test_ensure_checklist_fails_when_prompt_checklist_items_are_not_covered(task_artifacts):
    runtime = get_runtime_config()
    prompt_context = build_prompt_guardrail_context(
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.executor,
    )

    result = run_guardrails(
        phase_id="execute",
        step_name=SubRole.EXECUTOR,
        payload={"status": "PASS", "structured_output": {}},
        guardrails=["ensure_checklist"],
        task_context={**task_artifacts, **prompt_context},
        trace_id="test-prompt-checklist-fail",
    )

    assert result.status == PipelineStatus.NEEDS_FIX_EXECUTOR
    assert any("checklist_resolutions must be a list" in warning for warning in result.warnings)


def test_ensure_checklist_passes_when_prompt_checklist_items_are_covered(task_artifacts):
    runtime = get_runtime_config()
    prompt_context = build_prompt_guardrail_context(
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.executor,
    )
    checklist_items = prompt_context["guardrail_prompt_checklists"]

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
            "checklist_resolutions": [
                {"id": item["id"], "status": "done", "evidence": f"covered: {item['source']}"}
                for item in checklist_items
            ],
        },
        guardrails=["ensure_structured_output", "ensure_checklist"],
        task_context={**task_artifacts, **prompt_context},
        trace_id="test-prompt-checklist-pass",
    )

    assert result.status == PipelineStatus.PASS


def test_ensure_checklist_accepts_reason_alias_for_not_applicable_evidence(task_artifacts):
    runtime = get_runtime_config()
    prompt_context = build_prompt_guardrail_context(
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.executor,
    )
    checklist_items = prompt_context["guardrail_prompt_checklists"]

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
            "checklist_resolutions": [
                {
                    "id": item["id"],
                    "status": "not_applicable",
                    "reason": f"not applicable: {item['source']}",
                }
                for item in checklist_items
            ],
        },
        guardrails=["ensure_structured_output", "ensure_checklist"],
        task_context={**task_artifacts, **prompt_context},
        trace_id="test-prompt-checklist-reason-alias",
    )

    assert result.status == PipelineStatus.PASS
    assert result.warnings == []


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


def test_build_task_artifact_context_omits_missing_parent_task_card_for_phase_level(tmp_path: Path):
    task_dir = tmp_path / "2026-03-24_1800__multi-agent-system-design"
    task_dir.mkdir(parents=True, exist_ok=True)

    context = build_task_artifact_context(
        "2026-03-24_1800__multi-agent-system-design",
        task_dir_path=str(task_dir),
        task_card_path=str(task_dir / "TASK.md"),
    )

    assert "task_card_path" not in context
    assert "task_card_content" not in context
    assert context["task_dir_path"] == str(task_dir)


def test_build_task_artifact_context_keeps_parent_task_card_for_subtask_without_existing_file(tmp_path: Path):
    task_dir = tmp_path / "2026-03-24_1800__multi-agent-system-design"
    task_dir.mkdir(parents=True, exist_ok=True)

    context = build_task_artifact_context(
        "2026-03-24_1800__multi-agent-system-design",
        "devops-update-runtime-config",
        task_dir_path=str(task_dir),
        task_card_path=str(task_dir / "TASK.md"),
    )

    assert context["task_card_path"] == str(task_dir / "TASK.md")
    assert "task_card_content" not in context
    assert context["subtask_card_path"] == str(task_dir / "devops-update-runtime-config.md")


def test_build_task_artifact_context_includes_existing_card_contents(task_artifacts):
    Path(task_artifacts["task_card_path"]).write_text("# Task\n\ncontent")
    Path(task_artifacts["subtask_card_path"]).write_text("# Subtask\n\nsubcontent")

    context = build_task_artifact_context(
        "2026-03-24_1800__multi-agent-system-design",
        "devops-update-runtime-config",
        task_dir_path=task_artifacts["task_dir_path"],
        task_card_path=task_artifacts["task_card_path"],
        openhands_conversations_dir=task_artifacts["openhands_conversations_dir"],
    )

    assert context["task_card_content"] == "# Task\n\ncontent"
    assert context["subtask_card_content"] == "# Subtask\n\nsubcontent"


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


def test_apply_task_artifact_writes_replaces_task_card_content(task_artifacts):
    warnings = apply_task_artifact_writes(
        task_context={**task_artifacts, "trace_id": "test-artifact-write"},
        payload={
            "task_artifact_writes": [
                {
                    "path": task_artifacts["task_card_path"],
                    "mode": "full_replace",
                    "content": "# Task\n\nreplaced",
                }
            ]
        },
    )

    assert warnings == []
    assert Path(task_artifacts["task_card_path"]).read_text() == "# Task\n\nreplaced"


def test_apply_task_artifact_writes_rejects_non_task_paths(task_artifacts, tmp_path: Path):
    outside_path = tmp_path / "outside.md"
    warnings = apply_task_artifact_writes(
        task_context={**task_artifacts, "trace_id": "test-artifact-write-outside"},
        payload={
            "task_artifact_writes": [
                {
                    "path": str(outside_path),
                    "mode": "full_replace",
                    "content": "bad",
                }
            ]
        },
    )

    assert warnings
    assert not outside_path.exists()


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


def test_persist_openhands_conversation_artifact_keeps_multiple_reuse_attempts(task_artifacts):
    base_context = {
        **task_artifacts,
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "subtask_id": "devops-update-runtime-config",
    }
    first_path = persist_openhands_conversation_artifact(
        task_context=base_context,
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        conversation_id="conv-reuse",
        trace_id="test-conversation-persist-reuse-1",
        state={"execution_status": "RUNNING"},
        events={"events": [{"id": "evt-1"}]},
        raw_text="first attempt",
        parsed_payload={"status": "NEEDS_FIX_EXECUTOR"},
    )
    second_path = persist_openhands_conversation_artifact(
        task_context=base_context,
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        conversation_id="conv-reuse",
        trace_id="test-conversation-persist-reuse-2",
        state={"execution_status": "FINISHED"},
        events={"events": [{"id": "evt-2"}]},
        raw_text="second attempt",
        parsed_payload={"status": "PASS"},
    )

    assert first_path is not None
    assert second_path is not None
    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()
    first_saved = json.loads(first_path.read_text())
    second_saved = json.loads(second_path.read_text())
    assert first_saved["raw_text"] == "first attempt"
    assert second_saved["raw_text"] == "second attempt"


def test_persist_driver_step_artifacts_writes_summary_and_refs(task_artifacts):
    step_ref = persist_driver_step_artifacts(
        task_context={
            **task_artifacts,
            "task_id": "2026-03-24_1800__multi-agent-system-design",
            "subtask_id": "devops-update-runtime-config",
        },
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        attempt=1,
        trace_id="test-step-artifacts",
        status="PASS",
        request_artifact={
            "phase_id": "execute",
            "role_dir": "devops",
            "sub_role": "executor",
            "full_prompt": "do the thing",
        },
        raw_text="```yaml\nstatus: PASS\n```",
        parsed_payload={"status": "PASS", "summary": "done"},
        artifact_refs={},
    )

    assert step_ref is not None
    summary_path = Path(step_ref["summary_path"])
    assert summary_path.exists()
    summary = read_runtime_step_summary(str(summary_path))
    assert summary["phase_id"] == "execute"
    assert summary["sub_role"] == "executor"
    assert summary["status"] == "PASS"
    assert Path(summary["artifact_refs_path"]).exists()
    assert any(ref["artifact_kind"] == "prompt" for ref in summary["artifact_refs"])


def test_persist_cleanup_manifest_writes_explicit_cleanup_plan(task_artifacts):
    manifest_ref = persist_cleanup_manifest(
        state={
            "task_id": "2026-03-24_1800__multi-agent-system-design",
            "trace_id": "test-cleanup-manifest",
            "task_dir_path": task_artifacts["task_dir_path"],
            "task_worktree_root": task_artifacts["task_worktree_root"],
            "task_workspace_repos": task_artifacts["task_workspace_repos"],
            "methodology_root_runtime": "/tmp/methodology",
            "current_phase": "validate",
        },
        trace_id="test-cleanup-manifest",
    )

    assert manifest_ref is not None
    manifest_path = Path(manifest_ref["path"])
    assert manifest_path.exists()
    saved = json.loads(manifest_path.read_text())
    assert saved["cleanup_requires_explicit_user_approval"] is True
    assert saved["task_worktree_root"] == task_artifacts["task_worktree_root"]
