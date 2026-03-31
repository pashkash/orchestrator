"""Universal TaskUnit runner for collect/plan/execute/validate phases."""

from __future__ import annotations

from typing import Any

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverResult
from workflow_runtime.graph_compiler.state_schema import (
    FileChange,
    PipelineStatus,
    StructuredOutput,
    StructuredOutputStatus,
    SubRole,
    TaskUnitResult,
)
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.node_implementations.task_unit.executor_node import run_executor_step
from workflow_runtime.node_implementations.task_unit.guardrail_checker import run_guardrails
from workflow_runtime.node_implementations.task_unit.reviewer_node import run_reviewer_step
from workflow_runtime.node_implementations.task_unit.tester_node import run_tester_step


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.task_unit_runner._normalize_status:v1
# type: METHOD
# use_case: Normalizes raw driver status values into PipelineStatus.
# feature:
#   - Driver backends may return enum instances strings or missing statuses and TaskUnit needs one stable representation
# pre:
#   -
# post:
#   - returns a PipelineStatus value, defaulting to PASS when status is missing
# invariant:
#   - input value is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: status string is not a valid PipelineStatus value
# depends:
#   - PipelineStatus
# sft: normalize raw driver status values into a PipelineStatus enum with PASS as fallback
# idempotent: true
# logs: -
def _normalize_status(status: str | PipelineStatus | None) -> PipelineStatus:
    return PipelineStatus(str(status or PipelineStatus.PASS).upper())


# SEM_END orchestrator_v1.task_unit_runner._normalize_status:v1


# SEM_BEGIN orchestrator_v1.task_unit_runner._structured_output_from_payload:v1
# type: METHOD
# use_case: Converts a raw dict payload into a typed StructuredOutput.
# pre:
#   - payload may or may not contain a "structured_output" key (dict)
# post:
#   - returns StructuredOutput or None if the raw payload is invalid
# invariant:
#   - payload is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - StructuredOutput
#   - FileChange
# sft: convert raw driver payload dict into a typed StructuredOutput dataclass
# idempotent: true
# logs: -
def _structured_output_from_payload(payload: dict[str, Any]) -> StructuredOutput | None:
    raw_output = payload.get("structured_output")
    if not isinstance(raw_output, dict):
        return None
    return StructuredOutput(
        task_id=str(raw_output["task_id"]),
        subtask_id=str(raw_output["subtask_id"]),
        role=str(raw_output["role"]),
        status=StructuredOutputStatus(str(raw_output["status"])),
        changes=[
            FileChange(
                file=str(change["file"]),
                type=str(change["type"]),
                description=str(change["description"]),
            )
            for change in raw_output.get("changes", [])
        ],
        commands_executed=list(raw_output.get("commands_executed", [])),
        tests_passed=list(raw_output.get("tests_passed", [])),
        commits=list(raw_output.get("commits", [])),
        warnings=list(raw_output.get("warnings", [])),
        escalation=raw_output.get("escalation"),
        summary=str(raw_output.get("summary", "")),
    )


# SEM_END orchestrator_v1.task_unit_runner._structured_output_from_payload:v1


# SEM_BEGIN orchestrator_v1.task_unit_runner.task_unit_runner:v1
# type: CLASS
# use_case: Executes the universal TaskUnit pipeline for phase-level and subtask-level work.
# feature:
#   - V1 standardizes execution around executor reviewer optional tester and simple guardrails
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - one runner instance delegates all step execution to a single driver boundary
# modifies (internal):
#   -
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: driver execution failed
# depends:
#   - BaseDriver
# sft: implement universal task unit runner over executor reviewer tester and guardrails
# idempotent: false
# logs: query: TaskUnitRunner trace_id
class TaskUnitRunner:
    # SEM_BEGIN orchestrator_v1.task_unit_runner.task_unit_runner.__init__:v1
    # type: METHOD
    # use_case: Binds one runtime driver instance to a TaskUnitRunner.
    # feature:
    #   - All executor reviewer and tester steps for one runner share the same runtime backend
    # pre:
    #   - driver implements BaseDriver
    # post:
    #   - runner stores the provided driver for subsequent step execution
    # invariant:
    #   - driver reference is reused across all task unit calls made by this runner
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   -
    # depends:
    #   - BaseDriver
    # sft: initialize task unit runner with one runtime driver instance
    # idempotent: false
    # logs: -
    def __init__(self, driver: BaseDriver) -> None:
        self._driver = driver

    # SEM_END orchestrator_v1.task_unit_runner.task_unit_runner.__init__:v1

    # SEM_BEGIN orchestrator_v1.task_unit_runner._run_executor_with_retries:v1
    # type: METHOD
    # use_case: Retry loop for the executor step with guardrail checks after each attempt.
    # feature:
    #   - V1 TaskUnit allows N executor retries before escalation
    #   - orchestrator/config/phases_and_roles.yaml -> per-step max_retries
    # pre:
    #   - pipeline.executor.max_retries >= 1
    #   - workspace_root is not empty
    # post:
    #   - returns a DriverResult with the best executor step result
    # invariant:
    #   - task_context is not mutated
    # modifies (internal):
    #   - external.driver_runtime
    # emits (external):
    #   - external.driver_runtime
    # errors:
    #   - RuntimeError: all retries exhausted
    # depends:
    #   - run_executor_step
    #   - run_guardrails
    # sft: retry executor step with guardrail checks up to max_retries and return best result
    # idempotent: false
    # logs: query: TaskUnitRunner executor_attempt trace_id
    def _run_executor_with_retries(
        self,
        *,
        phase_id: str,
        role_dir: str,
        pipeline: PipelineConfig,
        task_context: dict[str, Any],
        workspace_root: str,
        metadata: dict[str, Any],
        trace_id: str,
    ) -> DriverResult:
        last_result: DriverResult | None = None
        for attempt in range(1, pipeline.executor.max_retries + 1):
            logger.info(
                "[TaskUnitRunner][_run_executor_with_retries][DecisionPoint] trace_id=%s | "
                "Branch: executor_attempt. Reason: phase=%s, role_dir=%s, attempt=%d/%d",
                trace_id,
                phase_id,
                role_dir,
                attempt,
                pipeline.executor.max_retries,
            )
            result = run_executor_step(
                driver=self._driver,
                phase_id=phase_id,
                role_dir=role_dir,
                step_config=pipeline.executor,
                task_context=task_context,
                workspace_root=workspace_root,
                metadata={**metadata, "trace_id": trace_id, "attempt": attempt},
            )
            last_result = result
            status = _normalize_status(result.status)
            guardrail_result = run_guardrails(
                phase_id=phase_id,
                step_name=SubRole.EXECUTOR,
                payload=result.payload,
                guardrails=pipeline.executor.guardrails,
                task_context=task_context,
                trace_id=trace_id,
            )
            if status == PipelineStatus.PASS and guardrail_result.status == PipelineStatus.PASS:
                return result
            if attempt == pipeline.executor.max_retries or status not in {
                PipelineStatus.PASS,
                PipelineStatus.NEEDS_FIX_EXECUTOR,
            }:
                return DriverResult(
                    status=guardrail_result.status if status == PipelineStatus.PASS else status,
                    payload={**result.payload, "warnings": guardrail_result.warnings or result.payload.get("warnings", [])},
                    raw_text=result.raw_text,
                    conversation_id=result.conversation_id,
                )
        return last_result or DriverResult(
            status=PipelineStatus.NEEDS_FIX_EXECUTOR,
            payload={"warnings": ["Executor returned no result"]},
        )

    # SEM_END orchestrator_v1.task_unit_runner._run_executor_with_retries:v1

    # SEM_BEGIN orchestrator_v1.task_unit_runner.run:v1
    # type: METHOD
    # use_case: Executes the universal TaskUnit for a single phase or subtask.
    # feature:
    #   - The same pipeline is used for collect/plan/execute/validate
    #   - V1 design dump: Executor -> Reviewer -> Guardrails -> Tester
    # pre:
    #   - pipeline.executor and pipeline.reviewer are defined
    #   - workspace_root is not empty
    # post:
    #   - returns a TaskUnitResult with a normalized PipelineStatus
    # invariant:
    #   - task_context is not mutated inside the runner
    # modifies (internal):
    #   - external.driver_runtime
    # emits (external):
    #   - external.driver_runtime
    # errors:
    #   - RuntimeError: driver execution failed
    # depends:
    #   - BaseDriver
    #   - run_executor_step
    #   - run_reviewer_step
    #   - run_tester_step
    #   - run_guardrails
    # sft: execute the universal task unit with executor reviewer tester and simple guardrails
    # idempotent: false
    # logs: query: task unit trace_id and step attempts
    def run(
        self,
        *,
        phase_id: str,
        role_dir: str,
        pipeline: PipelineConfig,
        task_context: dict[str, Any],
        workspace_root: str,
        metadata: dict[str, Any],
        trace_id: str | None = None,
    ) -> TaskUnitResult:
        resolved_trace_id = ensure_trace_id(trace_id)

        logger.info(
            "[TaskUnitRunner][run][ContextAnchor] trace_id=%s | "
            "Starting task unit. phase=%s, role_dir=%s",
            resolved_trace_id,
            phase_id,
            role_dir,
        )

        # === PRE[0]: workspace_root not empty ===
        logger.info(
            "[TaskUnitRunner][run][PreCheck] trace_id=%s | "
            "Checking workspace root is not empty. phase=%s, role_dir=%s",
            resolved_trace_id,
            phase_id,
            role_dir,
        )
        if not workspace_root:
            logger.warning(
                "[TaskUnitRunner][run][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
                "Workspace root is empty. phase=%s, role_dir=%s",
                resolved_trace_id,
                phase_id,
                role_dir,
            )
            return TaskUnitResult(
                status=PipelineStatus.BLOCKED,
                warnings=["workspace_root is empty"],
            )

        executor_result = self._run_executor_with_retries(
            phase_id=phase_id,
            role_dir=role_dir,
            pipeline=pipeline,
            task_context=task_context,
            workspace_root=workspace_root,
            metadata=metadata,
            trace_id=resolved_trace_id,
        )
        executor_status = _normalize_status(executor_result.status)
        if executor_status != PipelineStatus.PASS:
            logger.info(
                "[TaskUnitRunner][run][StepComplete] trace_id=%s | "
                "Task unit finished with executor status. phase=%s, role_dir=%s, status=%s",
                resolved_trace_id,
                phase_id,
                role_dir,
                executor_status,
            )
            return TaskUnitResult(
                status=executor_status,
                payload=executor_result.payload,
                structured_output=_structured_output_from_payload(executor_result.payload),
                warnings=list(executor_result.payload.get("warnings", [])),
                raw_text=executor_result.raw_text,
                conversation_id=executor_result.conversation_id,
            )

        reviewer_result = run_reviewer_step(
            driver=self._driver,
            phase_id=phase_id,
            role_dir=role_dir,
            step_config=pipeline.reviewer,
            task_context={**task_context, "executor_payload": executor_result.payload},
            workspace_root=workspace_root,
            metadata={**metadata, "trace_id": resolved_trace_id},
        )
        reviewer_guardrails = run_guardrails(
            phase_id=phase_id,
            step_name=SubRole.REVIEWER,
            payload=reviewer_result.payload,
            guardrails=pipeline.reviewer.guardrails,
            task_context=task_context,
            trace_id=resolved_trace_id,
        )
        reviewer_status = _normalize_status(
            reviewer_result.status
            if reviewer_guardrails.status == PipelineStatus.PASS
            else reviewer_guardrails.status
        )
        if reviewer_status != PipelineStatus.PASS:
            logger.info(
                "[TaskUnitRunner][run][StepComplete] trace_id=%s | "
                "Task unit finished with reviewer status. phase=%s, role_dir=%s, status=%s",
                resolved_trace_id,
                phase_id,
                role_dir,
                reviewer_status,
            )
            return TaskUnitResult(
                status=reviewer_status,
                payload=executor_result.payload,
                structured_output=_structured_output_from_payload(executor_result.payload),
                review_feedback=reviewer_result.payload.get("feedback"),
                warnings=reviewer_guardrails.warnings or list(reviewer_result.payload.get("warnings", [])),
                raw_text=reviewer_result.raw_text,
                conversation_id=reviewer_result.conversation_id or executor_result.conversation_id,
            )

        tester_summary = None
        tester_warnings: list[str] = []
        latest_conversation_id = reviewer_result.conversation_id or executor_result.conversation_id
        if pipeline.tester is not None:
            tester_result = run_tester_step(
                driver=self._driver,
                phase_id=phase_id,
                role_dir=role_dir,
                step_config=pipeline.tester,
                task_context={**task_context, "executor_payload": executor_result.payload},
                workspace_root=workspace_root,
                metadata={**metadata, "trace_id": resolved_trace_id},
            )
            tester_guardrails = run_guardrails(
                phase_id=phase_id,
                step_name=SubRole.TESTER,
                payload=tester_result.payload,
                guardrails=pipeline.tester.guardrails,
                task_context=task_context,
                trace_id=resolved_trace_id,
            )
            tester_status = _normalize_status(
                tester_result.status
                if tester_guardrails.status == PipelineStatus.PASS
                else tester_guardrails.status
            )
            latest_conversation_id = tester_result.conversation_id or latest_conversation_id
            if tester_status != PipelineStatus.PASS:
                logger.info(
                    "[TaskUnitRunner][run][StepComplete] trace_id=%s | "
                    "Task unit finished with tester status. phase=%s, role_dir=%s, status=%s",
                    resolved_trace_id,
                    phase_id,
                    role_dir,
                    tester_status,
                )
                return TaskUnitResult(
                    status=tester_status,
                    payload=executor_result.payload,
                    structured_output=_structured_output_from_payload(executor_result.payload),
                    review_feedback=reviewer_result.payload.get("feedback"),
                    test_summary=tester_result.payload.get("result"),
                    warnings=tester_guardrails.warnings or list(tester_result.payload.get("warnings", [])),
                    raw_text=tester_result.raw_text,
                    conversation_id=latest_conversation_id,
                )
            tester_summary = tester_result.payload.get("result")
            tester_warnings = list(tester_result.payload.get("warnings", []))

        logger.info(
            "[TaskUnitRunner][run][StepComplete] trace_id=%s | "
            "Task unit finished successfully. phase=%s, role_dir=%s",
            resolved_trace_id,
            phase_id,
            role_dir,
        )
        return TaskUnitResult(
            status=PipelineStatus.PASS,
            payload=executor_result.payload,
            structured_output=_structured_output_from_payload(executor_result.payload),
            review_feedback=reviewer_result.payload.get("feedback"),
            test_summary=tester_summary,
            warnings=list(executor_result.payload.get("warnings", [])) + tester_warnings,
            raw_text=executor_result.raw_text,
            conversation_id=latest_conversation_id,
        )

    # SEM_END orchestrator_v1.task_unit_runner.run:v1


# SEM_END orchestrator_v1.task_unit_runner.task_unit_runner:v1
