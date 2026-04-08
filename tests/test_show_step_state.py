from __future__ import annotations

from pathlib import Path

from workflow_runtime.integrations.tasks_storage import persist_driver_step_artifacts
from show_step_state import load_step_state


def test_load_step_state_returns_latest_attempt_summary(task_artifacts, monkeypatch):
    task_id = "2026-03-24_1800__multi-agent-system-design"
    monkeypatch.setattr(
        "show_step_state.resolve_task_directory",
        lambda requested_task_id: Path(task_artifacts["task_dir_path"]) if requested_task_id == task_id else Path("/missing"),
    )
    base_context = {
        **task_artifacts,
        "task_id": task_id,
        "subtask_id": "devops-update-runtime-config",
    }
    persist_driver_step_artifacts(
        task_context=base_context,
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        attempt=1,
        trace_id="test-show-step-state-1",
        status="NEEDS_FIX_EXECUTOR",
        request_artifact={"full_prompt": "first prompt"},
        raw_text="first raw",
        parsed_payload={"status": "NEEDS_FIX_EXECUTOR"},
        artifact_refs={},
    )
    persist_driver_step_artifacts(
        task_context=base_context,
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        attempt=2,
        trace_id="test-show-step-state-2",
        status="PASS",
        request_artifact={"full_prompt": "second prompt"},
        raw_text="second raw",
        parsed_payload={"status": "PASS", "summary": "done"},
        artifact_refs={},
    )

    loaded = load_step_state(
        task_id=task_id,
        phase_id="execute",
        subtask_id="devops-update-runtime-config",
        sub_role="executor",
    )

    assert loaded["summary"]["attempt"] == 2
    assert loaded["summary"]["status"] == "PASS"


def test_load_step_state_includes_artifact_contents(task_artifacts, monkeypatch):
    task_id = "2026-03-24_1800__multi-agent-system-design"
    monkeypatch.setattr(
        "show_step_state.resolve_task_directory",
        lambda requested_task_id: Path(task_artifacts["task_dir_path"]) if requested_task_id == task_id else Path("/missing"),
    )
    persist_driver_step_artifacts(
        task_context={
            **task_artifacts,
            "task_id": task_id,
            "subtask_id": "devops-update-runtime-config",
        },
        phase_id="execute",
        role_dir="devops",
        sub_role="executor",
        attempt=1,
        trace_id="test-show-step-state-include",
        status="PASS",
        request_artifact={"full_prompt": "inspect me"},
        raw_text="raw-output",
        parsed_payload={"status": "PASS", "summary": "done"},
        artifact_refs={},
    )

    loaded = load_step_state(
        task_id=task_id,
        phase_id="execute",
        subtask_id="devops-update-runtime-config",
        sub_role="executor",
        attempt=1,
        include_artifacts=True,
    )

    assert loaded["artifacts"]["prompt"]["content"] == "inspect me"
    assert loaded["artifacts"]["parsed_payload"]["content"]["status"] == "PASS"
