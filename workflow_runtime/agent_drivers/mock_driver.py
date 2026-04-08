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
from workflow_runtime.integrations.runtime_logging import get_logger

logger = get_logger(__name__)


def _mock_checklist_resolutions(task_context: dict[str, Any]) -> list[dict[str, str]]:
    checklist_items = task_context.get("guardrail_prompt_checklists", [])
    if not isinstance(checklist_items, list):
        return []
    return [
        {
            "id": str(item.get("id") or ""),
            "status": "done",
            "evidence": "mock-driver-covered",
        }
        for item in checklist_items
        if str(item.get("id") or "").strip()
    ]


# SEM_BEGIN orchestrator_v1.mock_driver.mock_driver:v1
# type: CLASS
# use_case: Deterministic runtime driver for tests and local dry-runs.
# feature:
#   - The phase graph can be verified without depending on real LLM quality or OpenHands availability
#   - Tests are exempt from SEM, but the runtime mock itself is production-facing support code for graph validation
# pre:
#   -
# post:
#   -
# invariant:
#   - identical DriverRequest inputs yield identical DriverResult outputs
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - DriverRequest
#   - DriverResult
# sft: implement deterministic mock runtime driver for orchestrator graph and task unit tests
# idempotent: true
# logs: command: uv run pytest tests/ -v
class MockDriver(BaseDriver):
    """Deterministic responses for collect/plan/execute/validate pipelines."""

    # SEM_BEGIN orchestrator_v1.mock_driver.mock_driver.run_task:v1
    # type: METHOD
    # use_case: Returns a deterministic mock payload for one phase/sub-role execution.
    # feature:
    #   - Mock mode validates routing, retries, and state transitions without a real runtime backend
    #   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
    # pre:
    #   - request.phase_id and request.sub_role are supported by the mock contract
    # post:
    #   - returns a DriverResult that matches the requested phase/sub-role contract
    # invariant:
    #   - request is not mutated
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   -
    # depends:
    #   - PipelineStatus
    #   - StructuredOutputStatus
    # sft: return deterministic mock driver result for one orchestrator phase or task unit step
    # idempotent: true
    # logs: command: uv run pytest tests/ -v
    def run_task(self, request: DriverRequest) -> DriverResult:
        trace_id = str(request.metadata.get("trace_id") or "mock-trace")
        checklist_resolutions = _mock_checklist_resolutions(request.task_context)
        logger.info(
            "[MockDriver][run_task][ContextAnchor] trace_id=%s | "
            "Resolving mock response. phase=%s, role_dir=%s, sub_role=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
        )
        if request.sub_role == SubRole.REVIEWER:
            logger.info(
                "[MockDriver][run_task][DecisionPoint] trace_id=%s | "
                "Branch: reviewer_pass. Reason: sub_role=reviewer",
                trace_id,
            )
            result = DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "feedback": f"Mock review PASS for {request.role_dir}/{request.phase_id}",
                    "checklist_resolutions": checklist_resolutions,
                    "warnings": [],
                },
                raw_text="mock-review-pass",
            )
            logger.info(
                "[MockDriver][run_task][StepComplete] trace_id=%s | "
                "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                result.status,
            )
            return result

        if request.sub_role == SubRole.TESTER:
            logger.info(
                "[MockDriver][run_task][DecisionPoint] trace_id=%s | "
                "Branch: tester_pass. Reason: sub_role=tester",
                trace_id,
            )
            result = DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "result": f"Mock tests PASS for {request.role_dir}/{request.phase_id}",
                    "tests_passed": [f"{request.phase_id}-{request.role_dir}-ok"],
                    "checklist_resolutions": checklist_resolutions,
                    "warnings": [],
                },
                raw_text="mock-tester-pass",
            )
            logger.info(
                "[MockDriver][run_task][StepComplete] trace_id=%s | "
                "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                result.status,
            )
            return result

        if request.phase_id == PhaseId.COLLECT:
            logger.info(
                "[MockDriver][run_task][DecisionPoint] trace_id=%s | "
                "Branch: collect_snapshot. Reason: phase_id=collect",
                trace_id,
            )
            result = DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "current_state": {
                        "git": {"branch": "main", "dirty": False},
                        "kubernetes": {"namespace": "squadder", "pods_unhealthy": []},
                        "runtime": {"mode": "mock"},
                    },
                    "checklist_resolutions": checklist_resolutions,
                    "warnings": [],
                },
                raw_text="mock-collect-pass",
            )
            logger.info(
                "[MockDriver][run_task][StepComplete] trace_id=%s | "
                "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                result.status,
            )
            return result

        if request.phase_id == PhaseId.PLAN:
            logger.info(
                "[MockDriver][run_task][DecisionPoint] trace_id=%s | "
                "Branch: plan_payload. Reason: phase_id=plan",
                trace_id,
            )
            result = DriverResult(
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
                    "checklist_resolutions": checklist_resolutions,
                    "warnings": [],
                },
                raw_text="mock-plan-pass",
            )
            logger.info(
                "[MockDriver][run_task][StepComplete] trace_id=%s | "
                "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                result.status,
            )
            return result

        if request.phase_id == PhaseId.VALIDATE:
            merged_summary: dict[str, Any] = request.task_context.get("merged_summary", {})
            conflicts = merged_summary.get("conflicts", [])
            if conflicts:
                logger.info(
                    "[MockDriver][run_task][DecisionPoint] trace_id=%s | "
                    "Branch: validate_replan. Reason: conflicts=%d",
                    trace_id,
                    len(conflicts),
                )
                result = DriverResult(
                    status=PipelineStatus.NEEDS_REPLAN,
                    payload={
                        "status": PipelineStatus.NEEDS_REPLAN,
                        "cross_cutting_result": PipelineStatus.NEEDS_REPLAN,
                        "final_result": None,
                        "checklist_resolutions": checklist_resolutions,
                        "warnings": conflicts,
                    },
                    raw_text="mock-validate-replan",
                )
                logger.info(
                    "[MockDriver][run_task][StepComplete] trace_id=%s | "
                    "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
                    trace_id,
                    request.phase_id,
                    request.role_dir,
                    request.sub_role,
                    result.status,
                )
                return result
            logger.info(
                "[MockDriver][run_task][DecisionPoint] trace_id=%s | "
                "Branch: validate_pass. Reason: conflicts=0",
                trace_id,
            )
            result = DriverResult(
                status=PipelineStatus.PASS,
                payload={
                    "status": PipelineStatus.PASS,
                    "cross_cutting_result": PipelineStatus.PASS,
                    "final_result": "Mock validation succeeded",
                    "checklist_resolutions": checklist_resolutions,
                    "warnings": [],
                },
                raw_text="mock-validate-pass",
            )
            logger.info(
                "[MockDriver][run_task][StepComplete] trace_id=%s | "
                "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                result.status,
            )
            return result

        task_id = str(request.metadata.get("task_id", "mock-task"))
        subtask_id = str(request.metadata.get("subtask_id", "mock-subtask"))
        role = request.role_dir
        result = DriverResult(
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
                "checklist_resolutions": checklist_resolutions,
                "warnings": [],
            },
            raw_text="mock-execute-pass",
        )
        logger.info(
            "[MockDriver][run_task][StepComplete] trace_id=%s | "
            "Resolved mock response. phase=%s, role_dir=%s, sub_role=%s, status=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            result.status,
        )
        return result

    # SEM_END orchestrator_v1.mock_driver.mock_driver.run_task:v1


# SEM_END orchestrator_v1.mock_driver.mock_driver:v1
