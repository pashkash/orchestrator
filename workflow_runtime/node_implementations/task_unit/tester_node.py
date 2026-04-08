"""Tester step wrapper for the universal TaskUnit."""

from __future__ import annotations

from typing import Any

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.graph_compiler.state_schema import PhaseId, SubRole
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.prompt_composer import compose_prompt, compose_prompt_parts
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.tester_node.run_tester_step:v1
# type: METHOD
# use_case: Builds the tester prompt and dispatches one tester step to the selected driver.
# feature:
#   - Universal TaskUnit can append an optional tester stage without changing phase-wrapper contracts
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4
# pre:
#   - step_config describes a tester prompt and model
#   - workspace_root is not empty
# post:
#   - returns the raw DriverResult for the tester step
# invariant:
#   - task_context and metadata are not mutated in-place
# modifies (internal):
#   - external.driver_runtime
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: driver execution failed
# depends:
#   - compose_prompt
#   - BaseDriver.run_task
# sft: compose tester prompt and run one tester step through the selected runtime driver
# idempotent: false
# logs: query: TaskUnitRunner trace_id
def run_tester_step(
    *,
    driver: BaseDriver,
    phase_id: PhaseId,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
    working_dir: str,
    metadata: dict[str, Any],
) -> DriverResult:
    trace_id = ensure_trace_id(metadata.get("trace_id"))
    logger.info(
        "[TesterNode][run_tester_step][ContextAnchor] trace_id=%s | "
        "Dispatching tester step. phase=%s, role_dir=%s, model=%s",
        trace_id,
        phase_id,
        role_dir,
        step_config.model,
    )
    system_prompt, user_prompt = compose_prompt_parts(
        phase_id=phase_id,
        role_dir=role_dir,
        step_config=step_config,
        task_context=task_context,
    )
    full_prompt = system_prompt + "\n\n" + user_prompt
    request_metadata = dict(metadata)
    runtime_overrides = dict(step_config.execution.runtime_overrides)
    if runtime_overrides:
        existing_overrides = request_metadata.get("execution_runtime_overrides")
        merged_overrides = dict(existing_overrides) if isinstance(existing_overrides, dict) else {}
        merged_overrides.update(runtime_overrides)
        request_metadata["execution_runtime_overrides"] = merged_overrides
    result = driver.run_task(
        DriverRequest(
            phase_id=phase_id,
            role_dir=role_dir,
            sub_role=SubRole.TESTER,
            execution_backend=step_config.execution.backend,
            execution_strategy=step_config.execution.strategy,
            model=step_config.model,
            prompt=full_prompt,
            system_prompt=system_prompt,
            task_context=task_context,
            working_dir=working_dir,
            metadata=request_metadata,
        )
    )
    logger.info(
        "[TesterNode][run_tester_step][StepComplete] trace_id=%s | "
        "Tester step dispatched. phase=%s, role_dir=%s, status=%s",
        trace_id,
        phase_id,
        role_dir,
        result.status,
    )
    return DriverResult(
        status=result.status,
        payload=result.payload,
        raw_text=result.raw_text,
        conversation_id=result.conversation_id,
        request_artifact={
            "phase_id": str(phase_id),
            "role_dir": role_dir,
            "sub_role": SubRole.TESTER.value,
            "model": step_config.model,
            "execution_backend": str(step_config.execution.backend),
            "execution_strategy": step_config.execution.strategy,
            "working_dir": working_dir,
            "metadata": request_metadata,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "full_prompt": full_prompt,
        },
        artifact_refs=result.artifact_refs,
    )


# SEM_END orchestrator_v1.tester_node.run_tester_step:v1
