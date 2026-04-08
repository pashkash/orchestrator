"""LangGraph-backed TaskUnit runtime for executor/guardrail/reviewer/tester flow."""

from __future__ import annotations

import json
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from lmnr import Laminar, observe

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverResult
from workflow_runtime.graph_compiler.state_schema import (
    FileChange,
    PhaseId,
    PipelineStatus,
    RuntimeArtifactRef,
    RuntimeStepRef,
    StructuredOutput,
    StructuredOutputStatus,
    SubRole,
    TaskUnitResult,
)
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineConfig, PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.prompt_composer import build_prompt_guardrail_context
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.integrations.tasks_storage import (
    apply_task_artifact_writes,
    build_task_artifact_context,
    persist_driver_step_artifacts,
    persist_guardrail_artifacts,
    persist_human_gate_artifact,
    persist_task_unit_result_artifact,
    sync_task_cards_from_structured_output,
)
from workflow_runtime.node_implementations.task_unit.executor_node import run_executor_step
from workflow_runtime.node_implementations.task_unit.guardrail_checker import GuardrailResult, run_guardrails
from workflow_runtime.node_implementations.task_unit.reviewer_node import run_reviewer_step
from workflow_runtime.node_implementations.task_unit.tester_node import run_tester_step


logger = get_logger(__name__)


class TaskUnitGraphState(TypedDict, total=False):
    step_attempts: dict[str, int]
    executor_attempts_used: int
    latest_step: str
    latest_step_context: dict[str, Any]
    latest_driver_result: DriverResult
    latest_guardrail_result: GuardrailResult
    latest_guardrail_feedback: str
    guardrail_failures: list[dict[str, Any]]
    latest_conversation_id: str | None
    executor_conversation_id: str | None
    downstream_task_context: dict[str, Any]
    executor_result: DriverResult
    reviewer_result: DriverResult
    tester_result: DriverResult
    runtime_step_refs: list[RuntimeStepRef]
    latest_step_ref_by_key: dict[str, RuntimeStepRef]
    latest_step_ref: RuntimeStepRef | None
    pending_approval_ref: RuntimeArtifactRef | None
    final_result: TaskUnitResult


def _normalize_status(status: str | PipelineStatus | None) -> PipelineStatus:
    return PipelineStatus(str(status or PipelineStatus.PASS).upper())


def _structured_output_from_payload(payload: dict[str, Any]) -> StructuredOutput | None:
    raw_output = payload.get("structured_output")
    if not isinstance(raw_output, dict):
        return None
    raw_status = str(raw_output.get("status", "")).strip().lower()
    status_aliases = {
        "done": StructuredOutputStatus.DONE,
        "completed": StructuredOutputStatus.DONE,
        "complete": StructuredOutputStatus.DONE,
        "pass": StructuredOutputStatus.DONE,
        "passed": StructuredOutputStatus.DONE,
        "success": StructuredOutputStatus.DONE,
        "ok": StructuredOutputStatus.DONE,
        "finished": StructuredOutputStatus.DONE,
        "failed": StructuredOutputStatus.FAILED,
        "fail": StructuredOutputStatus.FAILED,
        "error": StructuredOutputStatus.FAILED,
        "escalated": StructuredOutputStatus.ESCALATED,
        "escalate": StructuredOutputStatus.ESCALATED,
        "blocked": StructuredOutputStatus.ESCALATED,
        "cancelled": StructuredOutputStatus.CANCELLED,
        "canceled": StructuredOutputStatus.CANCELLED,
    }
    normalized_status = (
        status_aliases[raw_status]
        if raw_status in status_aliases
        else StructuredOutputStatus(raw_status)
    )
    return StructuredOutput(
        task_id=str(raw_output["task_id"]),
        subtask_id=str(raw_output["subtask_id"]),
        role=str(raw_output["role"]),
        status=normalized_status,
        changes=[
            FileChange(
                file=str(change.get("file") or change.get("path")),
                type=str(change.get("type") or change.get("action") or "modified"),
                description=str(
                    change.get("description")
                    or f"{change.get('type') or change.get('action') or 'modified'} "
                    f"{change.get('file') or change.get('path')}"
                ),
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


def _safe_review_feedback(reviewer_result: DriverResult | None) -> str | None:
    if reviewer_result is None:
        return None
    return reviewer_result.payload.get("feedback")


def _with_step_guardrail_context(
    *,
    base_task_context: dict[str, Any],
    role_dir: str,
    step_config: PipelineStepConfig,
) -> dict[str, Any]:
    return {
        **base_task_context,
        **build_prompt_guardrail_context(
            role_dir=role_dir,
            step_config=step_config,
        ),
    }


def _build_downstream_task_context(
    *,
    phase_id: str,
    task_context: dict[str, Any],
    executor_payload: dict[str, Any],
) -> dict[str, Any]:
    downstream_context = dict(task_context)
    if str(phase_id) == PhaseId.COLLECT:
        raw_current_state = executor_payload.get("current_state", {})
        downstream_context["current_state"] = (
            dict(raw_current_state) if isinstance(raw_current_state, dict) else {}
        )
        downstream_context["collector_result_meta"] = {
            "status": str(executor_payload.get("status", "")),
            "warnings": list(executor_payload.get("warnings", [])),
        }
        downstream_context.pop("executor_payload", None)
        return downstream_context
    downstream_context["executor_payload"] = executor_payload
    return downstream_context


def _attempts_key(step_name: SubRole) -> str:
    return str(step_name.value)


def _retryable_statuses(step_name: SubRole) -> set[PipelineStatus]:
    if step_name == SubRole.REVIEWER:
        return {PipelineStatus.PASS, PipelineStatus.NEEDS_FIX_REVIEW}
    if step_name == SubRole.TESTER:
        return {PipelineStatus.PASS, PipelineStatus.NEEDS_FIX_TESTS}
    return {PipelineStatus.PASS, PipelineStatus.NEEDS_FIX_EXECUTOR}


def _step_config_for(pipeline: PipelineConfig, step_name: SubRole) -> PipelineStepConfig | None:
    if step_name == SubRole.REVIEWER:
        return pipeline.reviewer
    if step_name == SubRole.TESTER:
        return pipeline.tester
    return pipeline.executor


def _failure_feedback(*, step_name: SubRole, attempt: int, warnings: list[str]) -> str:
    prefix = f"{step_name.value} guardrail attempt {attempt} failed"
    if not warnings:
        return prefix
    return prefix + ": " + "; ".join(warnings)


def _filtered_failures(state: TaskUnitGraphState, *, step_name: SubRole) -> list[dict[str, Any]]:
    return [
        failure
        for failure in state.get("guardrail_failures", [])
        if str(failure.get("step") or "") == step_name.value
    ]


def _task_context_with_retry_feedback(
    base_task_context: dict[str, Any],
    *,
    state: TaskUnitGraphState,
    step_name: SubRole,
) -> dict[str, Any]:
    retry_failures = _filtered_failures(state, step_name=step_name)
    if not retry_failures:
        return dict(base_task_context)
    latest_feedback = str(retry_failures[-1].get("feedback") or "").strip()
    updated = dict(base_task_context)
    updated["previous_guardrail_failures"] = retry_failures
    updated["latest_guardrail_feedback"] = latest_feedback
    updated["previous_feedback"] = latest_feedback
    return updated


def _is_execute_phase(phase_id: str) -> bool:
    return str(phase_id) == PhaseId.EXECUTE


def _build_human_question(
    *,
    phase_id: str,
    task_context: dict[str, Any],
    step_name: SubRole,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "source_phase": phase_id,
        "subtask_id": task_context.get("subtask_id"),
        "question": "; ".join(warnings) or f"{step_name.value} exhausted retry budget",
    }


def _build_failure_result(
    *,
    latest_step: SubRole,
    latest_driver_result: DriverResult,
    latest_guardrail_result: GuardrailResult | None,
    executor_result: DriverResult | None,
    reviewer_result: DriverResult | None,
    tester_result: DriverResult | None,
    executor_attempts_used: int,
    latest_conversation_id: str | None,
    human_question: dict[str, Any] | None = None,
) -> TaskUnitResult:
    latest_status = _normalize_status(latest_driver_result.status)
    guardrail_status = (
        latest_guardrail_result.status
        if latest_guardrail_result is not None
        else PipelineStatus.PASS
    )
    effective_status = (
        guardrail_status if latest_status == PipelineStatus.PASS and guardrail_status != PipelineStatus.PASS else latest_status
    )
    warnings = (
        list(latest_guardrail_result.warnings)
        if latest_guardrail_result is not None and latest_guardrail_result.status != PipelineStatus.PASS
        else list(latest_driver_result.payload.get("warnings", []))
    )
    effective_payload = (
        latest_driver_result.payload
        if latest_step == SubRole.EXECUTOR or executor_result is None
        else executor_result.payload
    )
    return TaskUnitResult(
        status=effective_status,
        payload=effective_payload,
        structured_output=_structured_output_from_payload(
            executor_result.payload if executor_result is not None else latest_driver_result.payload
        ),
        review_feedback=(
            reviewer_result.payload.get("feedback")
            if latest_step == SubRole.REVIEWER and reviewer_result is not None
            else _safe_review_feedback(reviewer_result)
        ),
        test_summary=tester_result.payload.get("result") if tester_result is not None else None,
        executor_attempts_used=executor_attempts_used,
        warnings=warnings,
        human_question=human_question,
        raw_text=latest_driver_result.raw_text,
        conversation_id=latest_conversation_id,
    )


def _append_runtime_step_ref(
    state: TaskUnitGraphState,
    *,
    step_ref: RuntimeStepRef | None,
) -> TaskUnitGraphState:
    if step_ref is None:
        return {}
    step_key = str(step_ref["step_key"])
    return {
        "runtime_step_refs": [*state.get("runtime_step_refs", []), step_ref],
        "latest_step_ref_by_key": {
            **state.get("latest_step_ref_by_key", {}),
            step_key: step_ref,
        },
        "latest_step_ref": step_ref,
    }


def _with_runtime_refs(
    *,
    result: TaskUnitResult,
    state: TaskUnitGraphState,
    pending_approval_ref: RuntimeArtifactRef | None = None,
) -> TaskUnitResult:
    return TaskUnitResult(
        status=result.status,
        payload=result.payload,
        structured_output=result.structured_output,
        review_feedback=result.review_feedback,
        test_summary=result.test_summary,
        executor_attempts_used=result.executor_attempts_used,
        warnings=list(result.warnings),
        human_question=result.human_question,
        raw_text=result.raw_text,
        conversation_id=result.conversation_id,
        runtime_step_refs=list(state.get("runtime_step_refs", [])),
        latest_step_ref_by_key=dict(state.get("latest_step_ref_by_key", {})),
        pending_approval_ref=pending_approval_ref,
    )


# SEM_BEGIN orchestrator_v1.task_unit_graph.run_task_unit_subgraph:v1
# type: METHOD
# use_case: Запускает universal TaskUnit subgraph для одного phase step и возвращает нормализованный TaskUnitResult.
# feature:
#   - Один и тот же subgraph обслуживает executor/reviewer/tester loop с guardrails, retries и escalation
#   - Executor retries в OpenHands reuse-ят conversation_id через state executor_conversation_id
# pre:
#   - pipeline.executor is configured
#   - task_context contains runtime roots needed by downstream nodes
# post:
#   - returns final TaskUnitResult produced by finish or human-gate node
# invariant:
#   - step routing always goes through one shared guardrail node
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.langgraph
#   - external.llm_provider
# errors:
#   - -
# depends:
#   - run_executor_step
#   - run_reviewer_step
#   - run_tester_step
#   - run_guardrails
# sft: run the reusable task unit langgraph with executor reviewer tester guardrails retries and human escalation
# idempotent: false
# logs: query: TaskUnitGraph run_task_unit_subgraph
@observe(name="task_unit_langgraph_workflow")
def run_task_unit_subgraph(
    *,
    driver: BaseDriver,
    phase_id: str,
    role_dir: str,
    pipeline: PipelineConfig,
    task_context: dict[str, Any],
    working_dir: str,
    metadata: dict[str, Any],
    trace_id: str,
) -> TaskUnitResult:
    resolved_trace_id = ensure_trace_id(trace_id)
    logger.info(
        "[TaskUnitGraph][run_task_unit_subgraph][ContextAnchor] trace_id=%s | "
        "Starting task-unit subgraph. phase=%s, role_dir=%s, working_dir=%s, reviewer_enabled=%s, tester_enabled=%s",
        resolved_trace_id,
        phase_id,
        role_dir,
        working_dir,
        pipeline.reviewer is not None,
        pipeline.tester is not None,
    )

    @observe(name="task_unit_executor_node")
    def executor_node(state: TaskUnitGraphState) -> TaskUnitGraphState:
        attempt = int(state.get("step_attempts", {}).get(_attempts_key(SubRole.EXECUTOR), 0)) + 1
        base_context = _task_context_with_retry_feedback(
            task_context,
            state=state,
            step_name=SubRole.EXECUTOR,
        )
        executor_task_context = _with_step_guardrail_context(
            base_task_context=base_context,
            role_dir=role_dir,
            step_config=pipeline.executor,
        )
        reuse_conversation_id = str(state.get("executor_conversation_id") or "").strip()
        executor_metadata = {
            **metadata,
            "trace_id": resolved_trace_id,
            "attempt": attempt,
            **(
                {"reuse_conversation_id": reuse_conversation_id}
                if reuse_conversation_id and attempt > 1
                else {}
            ),
        }
        result = run_executor_step(
            driver=driver,
            phase_id=PhaseId(str(phase_id)),
            role_dir=role_dir,
            step_config=pipeline.executor,
            task_context=executor_task_context,
            working_dir=working_dir,
            metadata=executor_metadata,
        )
        status = _normalize_status(result.status)
        structured_output = _structured_output_from_payload(result.payload)
        artifact_write_warnings: list[str] = []
        if status == PipelineStatus.PASS:
            artifact_write_warnings = apply_task_artifact_writes(
                task_context={**task_context, "trace_id": resolved_trace_id},
                payload=result.payload,
            )
            if structured_output is not None:
                sync_task_cards_from_structured_output(
                    task_context={**task_context, "trace_id": resolved_trace_id},
                    output=structured_output,
                )
        if artifact_write_warnings:
            result = DriverResult(
                status=result.status,
                payload={
                    **result.payload,
                    "warnings": list(result.payload.get("warnings", [])) + artifact_write_warnings,
                },
                raw_text=result.raw_text,
                conversation_id=result.conversation_id,
                request_artifact=result.request_artifact,
                artifact_refs=result.artifact_refs,
            )
        step_ref = persist_driver_step_artifacts(
            task_context=executor_task_context,
            phase_id=str(phase_id),
            role_dir=role_dir,
            sub_role=SubRole.EXECUTOR.value,
            attempt=attempt,
            trace_id=resolved_trace_id,
            status=str(status),
            request_artifact=result.request_artifact,
            raw_text=result.raw_text,
            parsed_payload=result.payload,
            artifact_refs=result.artifact_refs,
        )
        next_executor_conversation_id = (
            result.conversation_id
            if status == PipelineStatus.PASS and result.conversation_id
            else None
        )
        return {
            "step_attempts": {
                **state.get("step_attempts", {}),
                _attempts_key(SubRole.EXECUTOR): attempt,
            },
            "executor_attempts_used": int(state.get("executor_attempts_used", 0)) + 1,
            "latest_step": SubRole.EXECUTOR.value,
            "latest_step_context": executor_task_context,
            "latest_driver_result": result,
            "executor_result": result,
            "latest_conversation_id": result.conversation_id or state.get("latest_conversation_id"),
            "executor_conversation_id": next_executor_conversation_id,
            **_append_runtime_step_ref(state, step_ref=step_ref),
        }

    @observe(name="task_unit_reviewer_node")
    def reviewer_node(state: TaskUnitGraphState) -> TaskUnitGraphState:
        reviewer_config = pipeline.reviewer
        if reviewer_config is None:
            return {}
        attempt = int(state.get("step_attempts", {}).get(_attempts_key(SubRole.REVIEWER), 0)) + 1
        base_context = _task_context_with_retry_feedback(
            dict(state.get("downstream_task_context", {})),
            state=state,
            step_name=SubRole.REVIEWER,
        )
        reviewer_task_context = _with_step_guardrail_context(
            base_task_context=base_context,
            role_dir=role_dir,
            step_config=reviewer_config,
        )
        result = run_reviewer_step(
            driver=driver,
            phase_id=PhaseId(str(phase_id)),
            role_dir=role_dir,
            step_config=reviewer_config,
            task_context=reviewer_task_context,
            working_dir=working_dir,
            metadata={**metadata, "trace_id": resolved_trace_id, "attempt": attempt},
        )
        step_ref = persist_driver_step_artifacts(
            task_context=reviewer_task_context,
            phase_id=str(phase_id),
            role_dir=role_dir,
            sub_role=SubRole.REVIEWER.value,
            attempt=attempt,
            trace_id=resolved_trace_id,
            status=str(result.status),
            request_artifact=result.request_artifact,
            raw_text=result.raw_text,
            parsed_payload=result.payload,
            artifact_refs=result.artifact_refs,
        )
        return {
            "step_attempts": {
                **state.get("step_attempts", {}),
                _attempts_key(SubRole.REVIEWER): attempt,
            },
            "latest_step": SubRole.REVIEWER.value,
            "latest_step_context": reviewer_task_context,
            "latest_driver_result": result,
            "reviewer_result": result,
            "latest_conversation_id": result.conversation_id or state.get("latest_conversation_id"),
            **_append_runtime_step_ref(state, step_ref=step_ref),
        }

    @observe(name="task_unit_tester_node")
    def tester_node(state: TaskUnitGraphState) -> TaskUnitGraphState:
        tester_config = pipeline.tester
        if tester_config is None:
            return {}
        attempt = int(state.get("step_attempts", {}).get(_attempts_key(SubRole.TESTER), 0)) + 1
        base_context = _task_context_with_retry_feedback(
            dict(state.get("downstream_task_context", {})),
            state=state,
            step_name=SubRole.TESTER,
        )
        tester_task_context = _with_step_guardrail_context(
            base_task_context=base_context,
            role_dir=role_dir,
            step_config=tester_config,
        )
        result = run_tester_step(
            driver=driver,
            phase_id=PhaseId(str(phase_id)),
            role_dir=role_dir,
            step_config=tester_config,
            task_context=tester_task_context,
            working_dir=working_dir,
            metadata={**metadata, "trace_id": resolved_trace_id, "attempt": attempt},
        )
        step_ref = persist_driver_step_artifacts(
            task_context=tester_task_context,
            phase_id=str(phase_id),
            role_dir=role_dir,
            sub_role=SubRole.TESTER.value,
            attempt=attempt,
            trace_id=resolved_trace_id,
            status=str(result.status),
            request_artifact=result.request_artifact,
            raw_text=result.raw_text,
            parsed_payload=result.payload,
            artifact_refs=result.artifact_refs,
        )
        return {
            "step_attempts": {
                **state.get("step_attempts", {}),
                _attempts_key(SubRole.TESTER): attempt,
            },
            "latest_step": SubRole.TESTER.value,
            "latest_step_context": tester_task_context,
            "latest_driver_result": result,
            "tester_result": result,
            "latest_conversation_id": result.conversation_id or state.get("latest_conversation_id"),
            **_append_runtime_step_ref(state, step_ref=step_ref),
        }

    @observe(name="task_unit_guardrail_node")
    def guardrail_node(state: TaskUnitGraphState) -> TaskUnitGraphState:
        latest_step = SubRole(str(state["latest_step"]))
        latest_driver_result = state["latest_driver_result"]
        latest_step_context = dict(state.get("latest_step_context", {}))
        step_config = _step_config_for(pipeline, latest_step)
        guardrail_result = run_guardrails(
            phase_id=phase_id,
            step_name=latest_step,
            payload=latest_driver_result.payload,
            guardrails=list(step_config.guardrails if step_config is not None else []),
            task_context=latest_step_context,
            trace_id=resolved_trace_id,
        )
        updates: TaskUnitGraphState = {
            "latest_guardrail_result": guardrail_result,
            "latest_guardrail_feedback": "",
        }
        if guardrail_result.status != PipelineStatus.PASS:
            attempt = int(state.get("step_attempts", {}).get(_attempts_key(latest_step), 0))
            feedback = _failure_feedback(
                step_name=latest_step,
                attempt=attempt,
                warnings=list(guardrail_result.warnings),
            )
            updates["latest_guardrail_feedback"] = feedback
            updates["guardrail_failures"] = [
                *state.get("guardrail_failures", []),
                {
                    "step": latest_step.value,
                    "attempt": attempt,
                    "status": str(guardrail_result.status),
                    "warnings": list(guardrail_result.warnings),
                    "feedback": feedback,
                },
            ]
            tentative_state = {**state, **updates}
            persist_guardrail_artifacts(
                step_ref=state.get("latest_step_ref"),
                trace_id=resolved_trace_id,
                guardrail_payload={
                    "status": str(guardrail_result.status),
                    "warnings": list(guardrail_result.warnings),
                },
                route_decision=route_after_guardrail(tentative_state),
                feedback=feedback,
            )
            return updates
        if latest_step == SubRole.EXECUTOR:
            refreshed_task_context = {
                **task_context,
                **build_task_artifact_context(
                    str(task_context.get("task_id") or ""),
                    str(task_context.get("subtask_id") or "") or None,
                    task_dir_path=str(task_context.get("task_dir_path") or "") or None,
                    task_card_path=str(task_context.get("task_card_path") or "") or None,
                    openhands_conversations_dir=str(task_context.get("openhands_conversations_dir") or "")
                    or None,
                ),
            }
            updates["downstream_task_context"] = _build_downstream_task_context(
                phase_id=phase_id,
                task_context=refreshed_task_context,
                executor_payload=latest_driver_result.payload,
            )
        tentative_state = {**state, **updates}
        persist_guardrail_artifacts(
            step_ref=state.get("latest_step_ref"),
            trace_id=resolved_trace_id,
            guardrail_payload={
                "status": str(guardrail_result.status),
                "warnings": list(guardrail_result.warnings),
            },
            route_decision=route_after_guardrail(tentative_state),
            feedback=str(updates.get("latest_guardrail_feedback") or ""),
        )
        return updates

    def route_after_guardrail(state: TaskUnitGraphState) -> str:
        latest_step = SubRole(str(state["latest_step"]))
        latest_driver_result = state["latest_driver_result"]
        driver_status = _normalize_status(latest_driver_result.status)
        guardrail_result = state.get("latest_guardrail_result") or GuardrailResult(status=PipelineStatus.PASS)
        guardrail_status = guardrail_result.status
        if driver_status == PipelineStatus.PASS and guardrail_status == PipelineStatus.PASS:
            if latest_step == SubRole.EXECUTOR:
                if pipeline.reviewer is not None:
                    return "reviewer"
                if pipeline.tester is not None:
                    return "tester"
                return "finish"
            if latest_step == SubRole.REVIEWER:
                return "tester" if pipeline.tester is not None else "finish"
            return "finish"
        if driver_status in {
            PipelineStatus.ASK_HUMAN,
            PipelineStatus.ESCALATE_TO_HUMAN,
            PipelineStatus.BLOCKED,
            PipelineStatus.NEEDS_INFO,
            PipelineStatus.NEEDS_MORE_SNAPSHOT,
            PipelineStatus.NEEDS_REPLAN,
        }:
            return "finish"
        attempts = int(state.get("step_attempts", {}).get(_attempts_key(latest_step), 0))
        step_config = _step_config_for(pipeline, latest_step)
        max_retries = int(step_config.max_retries if step_config is not None else 1)
        if attempts < max_retries and driver_status in _retryable_statuses(latest_step):
            return latest_step.value
        if _is_execute_phase(phase_id):
            return "task_unit_human_gate"
        return "finish"

    @observe(name="task_unit_human_gate")
    def task_unit_human_gate_node(state: TaskUnitGraphState) -> TaskUnitGraphState:
        latest_step = SubRole(str(state["latest_step"]))
        latest_driver_result = state["latest_driver_result"]
        latest_guardrail_result = state.get("latest_guardrail_result")
        human_question = _build_human_question(
            phase_id=phase_id,
            task_context=task_context,
            step_name=latest_step,
            warnings=list(latest_guardrail_result.warnings if latest_guardrail_result is not None else []),
        )
        failure_result = _build_failure_result(
            latest_step=latest_step,
            latest_driver_result=latest_driver_result,
            latest_guardrail_result=latest_guardrail_result,
            executor_result=state.get("executor_result"),
            reviewer_result=state.get("reviewer_result"),
            tester_result=state.get("tester_result"),
            executor_attempts_used=int(state.get("executor_attempts_used", 0)),
            latest_conversation_id=state.get("latest_conversation_id"),
            human_question=human_question,
        )
        pending_approval_ref = persist_human_gate_artifact(
            task_context=task_context,
            phase_id=str(phase_id),
            subtask_id=task_context.get("subtask_id"),
            attempt=int((state.get("latest_step_ref") or {}).get("attempt", 1)),
            trace_id=resolved_trace_id,
            artifact_kind="human_gate_question",
            payload=human_question,
            summary_path=str((state.get("latest_step_ref") or {}).get("summary_path") or "") or None,
        )
        final_result = _with_runtime_refs(
            result=TaskUnitResult(
                status=PipelineStatus.ESCALATE_TO_HUMAN,
                payload=failure_result.payload,
                structured_output=failure_result.structured_output,
                review_feedback=failure_result.review_feedback,
                test_summary=failure_result.test_summary,
                executor_attempts_used=failure_result.executor_attempts_used,
                warnings=failure_result.warnings,
                human_question=human_question,
                raw_text=failure_result.raw_text,
                conversation_id=failure_result.conversation_id,
            ),
            state=state,
            pending_approval_ref=pending_approval_ref,
        )
        persist_task_unit_result_artifact(
            step_ref=state.get("latest_step_ref"),
            trace_id=resolved_trace_id,
            task_unit_result=final_result,
        )
        return {"final_result": final_result, "pending_approval_ref": pending_approval_ref}

    @observe(name="task_unit_finish")
    def finish_node(state: TaskUnitGraphState) -> TaskUnitGraphState:
        latest_step = SubRole(str(state["latest_step"]))
        latest_driver_result = state["latest_driver_result"]
        latest_guardrail_result = state.get("latest_guardrail_result")
        final_result = _build_failure_result(
            latest_step=latest_step,
            latest_driver_result=latest_driver_result,
            latest_guardrail_result=latest_guardrail_result,
            executor_result=state.get("executor_result"),
            reviewer_result=state.get("reviewer_result"),
            tester_result=state.get("tester_result"),
            executor_attempts_used=int(state.get("executor_attempts_used", 0)),
            latest_conversation_id=state.get("latest_conversation_id"),
        )
        human_question = final_result.human_question
        if human_question is None and final_result.status in {
            PipelineStatus.ASK_HUMAN,
            PipelineStatus.ESCALATE_TO_HUMAN,
            PipelineStatus.BLOCKED,
        }:
            human_question = _build_human_question(
                phase_id=phase_id,
                task_context=task_context,
                step_name=latest_step,
                warnings=list(final_result.warnings),
            )
        pending_approval_ref = (
            persist_human_gate_artifact(
                task_context=task_context,
                phase_id=str(phase_id),
                subtask_id=task_context.get("subtask_id"),
                attempt=int((state.get("latest_step_ref") or {}).get("attempt", 1)),
                trace_id=resolved_trace_id,
                artifact_kind="human_gate_question",
                payload=human_question,
                summary_path=str((state.get("latest_step_ref") or {}).get("summary_path") or "") or None,
            )
            if human_question is not None
            else None
        )
        final_result = _with_runtime_refs(
            result=TaskUnitResult(
                status=final_result.status,
                payload=final_result.payload,
                structured_output=final_result.structured_output,
                review_feedback=final_result.review_feedback,
                test_summary=final_result.test_summary,
                executor_attempts_used=final_result.executor_attempts_used,
                warnings=final_result.warnings,
                human_question=human_question,
                raw_text=final_result.raw_text,
                conversation_id=final_result.conversation_id,
            ),
            state=state,
            pending_approval_ref=pending_approval_ref,
        )
        persist_task_unit_result_artifact(
            step_ref=state.get("latest_step_ref"),
            trace_id=resolved_trace_id,
            task_unit_result=final_result,
        )
        return {"final_result": final_result, "pending_approval_ref": pending_approval_ref}

    graph_builder = StateGraph(TaskUnitGraphState)
    graph_builder.add_node("executor", executor_node)
    graph_builder.add_node("guardrail", guardrail_node)
    graph_builder.add_node("reviewer", reviewer_node)
    graph_builder.add_node("tester", tester_node)
    graph_builder.add_node("task_unit_human_gate", task_unit_human_gate_node)
    graph_builder.add_node("finish", finish_node)
    graph_builder.add_edge(START, "executor")
    graph_builder.add_edge("executor", "guardrail")
    graph_builder.add_edge("reviewer", "guardrail")
    graph_builder.add_edge("tester", "guardrail")
    graph_builder.add_conditional_edges(
        "guardrail",
        route_after_guardrail,
        {
            "executor": "executor",
            "reviewer": "reviewer",
            "tester": "tester",
            "task_unit_human_gate": "task_unit_human_gate",
            "finish": "finish",
        },
    )
    graph_builder.add_edge("task_unit_human_gate", END)
    graph_builder.add_edge("finish", END)

    graph = graph_builder.compile()
    # SEM_BEGIN section.orchestrator_v1.task_unit_graph.attach_graph_attrs:v1
    # type: SECTION
    # use_case: Материализует topology task-unit subgraph в span attrs, которые ожидает Laminar UI.
    # feature:
    #   - task_unit_langgraph_workflow должен отдавать nodes/edges явно, иначе subgraph не виден в UI
    # pre:
    #   - graph exposes get_graph()
    # post:
    #   - current span receives langgraph.nodes and langgraph.edges on success
    # invariant:
    #   - task-unit graph topology is not mutated
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   - -
    # depends:
    #   - CompiledStateGraph.get_graph
    # sft: attach task unit graph nodes and edges to laminar span attributes
    # idempotent: true
    # logs: query: task_unit_graph attach_graph_attrs
    try:
        g = graph.get_graph()
        nodes = [{"id": n.id, "name": n.name, "metadata": n.metadata} for n in g.nodes.values()]
        edges = [
            {"source": e.source, "target": e.target, "conditional": e.conditional}
            for e in g.edges
        ]
        Laminar.set_span_attributes(
            {
                "langgraph.nodes": json.dumps(nodes),
                "langgraph.edges": json.dumps(edges),
            }
        )
        logger.info(
            "[TaskUnitGraph][run_task_unit_subgraph][StepComplete] trace_id=%s | "
            "Task-unit graph attributes attached. nodes=%d, edges=%d",
            resolved_trace_id,
            len(nodes),
            len(edges),
        )
    except Exception:
        logger.error(
            "[TaskUnitGraph][run_task_unit_subgraph][ErrorHandled][ERR:UNEXPECTED] trace_id=%s | "
            "Failed to attach task-unit graph attributes.",
            resolved_trace_id,
            exc_info=True,
        )
    # SEM_END section.orchestrator_v1.task_unit_graph.attach_graph_attrs:v1
    initial_state: TaskUnitGraphState = {
        "step_attempts": {},
        "executor_attempts_used": 0,
        "guardrail_failures": [],
        "latest_guardrail_feedback": "",
        "latest_conversation_id": None,
        "executor_conversation_id": None,
        "downstream_task_context": {},
        "runtime_step_refs": [],
        "latest_step_ref_by_key": {},
        "latest_step_ref": None,
        "pending_approval_ref": None,
    }
    result_state = graph.invoke(
        initial_state,
        {"recursion_limit": max(10, pipeline.executor.max_retries + (pipeline.reviewer.max_retries if pipeline.reviewer else 0) + (pipeline.tester.max_retries if pipeline.tester else 0) + 6)},
    )
    final_result = result_state.get("final_result")
    if isinstance(final_result, TaskUnitResult):
        logger.info(
            "[TaskUnitGraph][run_task_unit_subgraph][StepComplete] trace_id=%s | "
            "Task-unit subgraph finished. phase=%s, role_dir=%s, final_status=%s, executor_attempts_used=%d",
            resolved_trace_id,
            phase_id,
            role_dir,
            final_result.status,
            final_result.executor_attempts_used,
        )
        return final_result
    logger.warning(
        "[TaskUnitGraph][run_task_unit_subgraph][ErrorHandled][ERR:POSTCONDITION] trace_id=%s | "
        "TaskUnit subgraph returned without final_result. phase=%s, role_dir=%s",
        resolved_trace_id,
        phase_id,
        role_dir,
    )
    return TaskUnitResult(
        status=PipelineStatus.BLOCKED,
        warnings=["TaskUnit subgraph returned without final_result"],
    )
# SEM_END orchestrator_v1.task_unit_graph.run_task_unit_subgraph:v1
