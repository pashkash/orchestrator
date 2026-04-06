"""Phase recovery and escalation tests for the orchestrator runtime."""

from __future__ import annotations

from workflow_runtime.agent_drivers.base_driver import DriverResult
from workflow_runtime.graph_compiler.state_schema import (
    PhaseId,
    PipelineStatus,
    StructuredOutputStatus,
    SubtaskState,
    SubtaskStatus,
)
from workflow_runtime.integrations.phase_config_loader import get_runtime_config
from workflow_runtime.node_implementations.phases.execute_phase import run_execute_phase
from workflow_runtime.node_implementations.phases.plan_phase import _merge_plan
from workflow_runtime.node_implementations.task_unit.runner import (
    TaskUnitRunner,
    _structured_output_from_payload,
)
from tests.mocks import ScriptedDriver


def test_merge_plan_resets_non_done_subtask_for_retry():
    existing = SubtaskState(
        id="backend-create-square-file",
        role="backend",
        description="Old description",
        status=SubtaskStatus.BLOCKED,
        retry_count=1,
        max_retries=3,
        reviewer_feedback="Old feedback",
        tester_result="Old tester result",
        escalation_reason="Old escalation",
    )

    merged = _merge_plan(
        [existing],
        [
            {
                "id": "backend-create-square-file",
                "role": "backend",
                "description": "Updated description",
                "dependencies": [],
                "max_retries": 3,
            }
        ],
    )

    assert len(merged) == 1
    assert merged[0].status == SubtaskStatus.PENDING
    assert merged[0].description == "Updated description"
    assert merged[0].retry_count == 1
    assert merged[0].reviewer_feedback is None
    assert merged[0].tester_result is None
    assert merged[0].escalation_reason is None


def test_structured_output_from_payload_normalizes_status_and_path_alias():
    structured_output = _structured_output_from_payload(
        {
            "structured_output": {
                "task_id": "task-1",
                "subtask_id": "backend-create-square-py",
                "role": "backend / executor",
                "status": "COMPLETED",
                "changes": [
                    {
                        "path": "/tmp/square.py",
                        "action": "created",
                    }
                ],
                "commands_executed": [],
                "tests_passed": [],
                "commits": [],
                "warnings": [],
                "escalation": None,
                "summary": "Created square.py",
            }
        }
    )

    assert structured_output is not None
    assert structured_output.status == StructuredOutputStatus.DONE
    assert structured_output.changes[0].file == "/tmp/square.py"
    assert structured_output.changes[0].type == "created"
    assert structured_output.changes[0].description == "created /tmp/square.py"


def test_execute_phase_escalates_after_retry_budget_exhausted(task_artifacts):
    runtime = get_runtime_config()
    task_unit_runner = TaskUnitRunner(
        ScriptedDriver(
            {
                ("execute", "executor", "backend"): [
                    DriverResult(
                        status="NEEDS_FIX_EXECUTOR",
                        payload={
                            "status": "NEEDS_FIX_EXECUTOR",
                            "warnings": ["Executor returned non-YAML final output"],
                        },
                        raw_text="plain text finish",
                    ),
                    DriverResult(
                        status="NEEDS_FIX_EXECUTOR",
                        payload={
                            "status": "NEEDS_FIX_EXECUTOR",
                            "warnings": ["Executor returned non-YAML final output"],
                        },
                        raw_text="plain text finish",
                    ),
                    DriverResult(
                        status="NEEDS_FIX_EXECUTOR",
                        payload={
                            "status": "NEEDS_FIX_EXECUTOR",
                            "warnings": ["Executor returned non-YAML final output"],
                        },
                        raw_text="plain text finish",
                    ),
                ]
            }
        )
    )
    state = {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Create square.py",
        "workspace_root": "/root/squadder-devops",
        "task_worktree_root": task_artifacts["task_worktree_root"],
        "trace_id": "test-execute-escalation",
        "task_dir_path": task_artifacts["task_dir_path"],
        "task_card_path": task_artifacts["task_card_path"],
        "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
        "current_state": {},
        "structured_outputs": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {"execute": 2},
        "plan": [
            SubtaskState(
                id="backend-create-square-file",
                role="backend",
                description="Create square.py",
                retry_count=2,
                max_retries=3,
            )
        ],
    }

    result = run_execute_phase(
        state,
        task_unit_runner=task_unit_runner,
        phase_config=runtime.phases[PhaseId.EXECUTE],
    )

    assert result["current_status"] == PipelineStatus.ESCALATE_TO_HUMAN
    assert result["active_subtask_id"] == "backend-create-square-file"
    assert result["plan"][0].status == SubtaskStatus.ESCALATED
    assert "pending_human_input" in result


def test_execute_phase_escalates_when_task_unit_consumes_full_executor_budget(task_artifacts):
    runtime = get_runtime_config()
    task_unit_runner = TaskUnitRunner(
        ScriptedDriver(
            {
                ("execute", "executor", "backend"): [
                    DriverResult(
                        status="NEEDS_FIX_EXECUTOR",
                        payload={
                            "status": "NEEDS_FIX_EXECUTOR",
                            "warnings": ["Executor returned non-YAML final output"],
                        },
                        raw_text="plain text finish",
                    ),
                    DriverResult(
                        status="NEEDS_FIX_EXECUTOR",
                        payload={
                            "status": "NEEDS_FIX_EXECUTOR",
                            "warnings": ["Executor returned non-YAML final output"],
                        },
                        raw_text="plain text finish",
                    ),
                    DriverResult(
                        status="NEEDS_FIX_EXECUTOR",
                        payload={
                            "status": "NEEDS_FIX_EXECUTOR",
                            "warnings": ["Executor returned non-YAML final output"],
                        },
                        raw_text="plain text finish",
                    ),
                ]
            }
        )
    )
    state = {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Create square.py",
        "workspace_root": "/root/squadder-devops",
        "task_worktree_root": task_artifacts["task_worktree_root"],
        "trace_id": "test-execute-attempt-budget",
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
                id="backend-create-square-file",
                role="backend",
                description="Create square.py",
                retry_count=0,
                max_retries=3,
            )
        ],
    }

    result = run_execute_phase(
        state,
        task_unit_runner=task_unit_runner,
        phase_config=runtime.phases[PhaseId.EXECUTE],
    )

    assert result["current_status"] == PipelineStatus.ESCALATE_TO_HUMAN
    assert result["plan"][0].status == SubtaskStatus.ESCALATED
    assert result["plan"][0].retry_count == 3
    assert "pending_human_input" in result


def test_execute_phase_does_not_false_pass_when_only_blocked_subtasks_remain(task_artifacts):
    runtime = get_runtime_config()
    task_unit_runner = TaskUnitRunner(ScriptedDriver({}))
    state = {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Create square.py",
        "workspace_root": "/root/squadder-devops",
        "task_worktree_root": task_artifacts["task_worktree_root"],
        "trace_id": "test-execute-blocked-plan",
        "task_dir_path": task_artifacts["task_dir_path"],
        "task_card_path": task_artifacts["task_card_path"],
        "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
        "current_state": {},
        "structured_outputs": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {"execute": 1},
        "plan": [
            SubtaskState(
                id="backend-create-square-file",
                role="backend",
                description="Create square.py",
                status=SubtaskStatus.BLOCKED,
                retry_count=1,
                max_retries=3,
                escalation_reason="Executor returned non-YAML final output",
            )
        ],
    }

    result = run_execute_phase(
        state,
        task_unit_runner=task_unit_runner,
        phase_config=runtime.phases[PhaseId.EXECUTE],
    )

    assert result["current_status"] == PipelineStatus.ESCALATE_TO_HUMAN
    assert result["active_subtask_id"] == "backend-create-square-file"
