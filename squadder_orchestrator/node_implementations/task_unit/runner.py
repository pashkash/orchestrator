"""Universal TaskUnit runner for collect/plan/execute/validate phases."""

from __future__ import annotations

import logging
from typing import Any

from squadder_orchestrator.agent_drivers.base_driver import BaseDriver, DriverResult
from squadder_orchestrator.graph_compiler.state_schema import (
    FileChange,
    PipelineStatus,
    StructuredOutput,
    StructuredOutputStatus,
    SubRole,
    TaskUnitResult,
)
from squadder_orchestrator.graph_compiler.yaml_manifest_parser import PipelineConfig
from squadder_orchestrator.integrations.observability import ensure_trace_id
from squadder_orchestrator.node_implementations.task_unit.executor_node import run_executor_step
from squadder_orchestrator.node_implementations.task_unit.guardrail_checker import run_guardrails
from squadder_orchestrator.node_implementations.task_unit.reviewer_node import run_reviewer_step
from squadder_orchestrator.node_implementations.task_unit.tester_node import run_tester_step


logger = logging.getLogger(__name__)


def _normalize_status(status: str | PipelineStatus | None) -> PipelineStatus:
    return PipelineStatus(str(status or PipelineStatus.PASS).upper())


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


class TaskUnitRunner:
    def __init__(self, driver: BaseDriver) -> None:
        self._driver = driver

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

    # SEM_BEGIN orchestrator_v1.task_unit_runner.run:v1
    # type: METHOD
    # use_case: Выполняет universal TaskUnit для одной фазы или одной subtask.
    # feature:
    #   - Один и тот же pipeline используется для collect/plan/execute/validate
    #   - V1 design dump: Executor -> Reviewer -> Guardrails -> Tester
    # pre:
    #   - pipeline.executor и pipeline.reviewer определены
    #   - workspace_root не пустой
    # post:
    #   - возвращает TaskUnitResult с нормализованным PipelineStatus
    # invariant:
    #   - task_context не мутируется внутри runner-а
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
