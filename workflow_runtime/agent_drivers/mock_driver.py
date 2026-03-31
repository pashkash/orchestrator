"""Deterministic mock driver for V1 tests and local dry-runs."""

from __future__ import annotations

from typing import Any

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.graph_compiler.state_schema import (
    PhaseId,
    PipelineStatus,
    StructuredOutputStatus,
    SubRole,
)


class MockDriver(BaseDriver):
    """Deterministic responses for collect/plan/execute/validate pipelines."""

    def run_task(self, request: DriverRequest) -> DriverResult:
        if request.sub_role == SubRole.REVIEWER:
            return DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "feedback": f"Mock review PASS for {request.role_dir}/{request.phase_id}",
                    "warnings": [],
                },
                raw_text="mock-review-pass",
            )

        if request.sub_role == SubRole.TESTER:
            return DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "result": f"Mock tests PASS for {request.role_dir}/{request.phase_id}",
                    "tests_passed": [f"{request.phase_id}-{request.role_dir}-ok"],
                    "warnings": [],
                },
                raw_text="mock-tester-pass",
            )

        if request.phase_id == PhaseId.COLLECT:
            return DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "current_state": {
                        "git": {"branch": "main", "dirty": False},
                        "kubernetes": {"namespace": "squadder", "pods_unhealthy": []},
                        "runtime": {"mode": "mock"},
                    },
                    "warnings": [],
                },
                raw_text="mock-collect-pass",
            )

        if request.phase_id == PhaseId.PLAN:
            return DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "plan": [
                        {
                            "id": "devops-update-runtime-config",
                            "role": "devops",
                            "description": "Update orchestrator runtime manifests",
                            "dependencies": [],
                            "max_retries": 3,
                        },
                        {
                            "id": "backend-wire-phase-runtime",
                            "role": "backend",
                            "description": "Wire runtime config into prompt and phase loaders",
                            "dependencies": ["devops-update-runtime-config"],
                            "max_retries": 3,
                        },
                    ],
                    "warnings": [],
                },
                raw_text="mock-plan-pass",
            )

        if request.phase_id == PhaseId.VALIDATE:
            merged_summary: dict[str, Any] = request.task_context.get("merged_summary", {})
            conflicts = merged_summary.get("conflicts", [])
            if conflicts:
                return DriverResult(
                    status=PipelineStatus.NEEDS_REPLAN,
                    payload={
                        "status": PipelineStatus.NEEDS_REPLAN,
                        "cross_cutting_result": PipelineStatus.NEEDS_REPLAN,
                        "final_result": None,
                        "warnings": conflicts,
                    },
                    raw_text="mock-validate-replan",
                )
            return DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "cross_cutting_result": PipelineStatus.PASS,
                    "final_result": "Mock validation succeeded",
                    "warnings": [],
                },
                raw_text="mock-validate-pass",
            )

        task_id = str(request.metadata.get("task_id", "mock-task"))
        subtask_id = str(request.metadata.get("subtask_id", "mock-subtask"))
        role = request.role_dir
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "structured_output": {
                    "task_id": task_id,
                    "subtask_id": subtask_id,
                    "role": role,
                    "status": StructuredOutputStatus.DONE,
                    "changes": [
                        {
                            "file": f"{role}/artifact.txt",
                            "type": "modified",
                            "description": f"Mock change produced by {role}",
                        }
                    ],
                    "commands_executed": [f"mock-{role}-command"],
                    "tests_passed": [],
                    "commits": [],
                    "warnings": [],
                    "escalation": None,
                    "summary": f"Mock execution completed for {subtask_id}",
                },
                "warnings": [],
            },
            raw_text="mock-execute-pass",
        )
