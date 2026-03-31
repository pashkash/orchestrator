"""Tester step wrapper for the universal TaskUnit."""

from __future__ import annotations

from typing import Any

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.graph_compiler.state_schema import PhaseId, SubRole
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.prompt_composer import compose_prompt
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
    workspace_root: str,
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
    prompt = compose_prompt(
        phase_id=phase_id,
        role_dir=role_dir,
        step_config=step_config,
        task_context=task_context,
    )
    result = driver.run_task(
        DriverRequest(
            phase_id=phase_id,
            role_dir=role_dir,
            sub_role=SubRole.TESTER,
            model=step_config.model,
            prompt=prompt,
            task_context=task_context,
            workspace_root=workspace_root,
            metadata=metadata,
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
    return result


# SEM_END orchestrator_v1.tester_node.run_tester_step:v1
