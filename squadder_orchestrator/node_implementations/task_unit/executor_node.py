"""Executor step wrapper for the universal TaskUnit."""

from __future__ import annotations

from typing import Any

from squadder_orchestrator.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from squadder_orchestrator.graph_compiler.state_schema import PhaseId, SubRole
from squadder_orchestrator.graph_compiler.yaml_manifest_parser import PipelineStepConfig
from squadder_orchestrator.integrations.prompt_composer import compose_prompt


def run_executor_step(
    *,
    driver: BaseDriver,
    phase_id: PhaseId,
    role_dir: str,
    step_config: PipelineStepConfig,
    task_context: dict[str, Any],
    workspace_root: str,
    metadata: dict[str, Any],
) -> DriverResult:
    prompt = compose_prompt(
        phase_id=phase_id,
        role_dir=role_dir,
        step_config=step_config,
        task_context=task_context,
    )
    return driver.run_task(
        DriverRequest(
            phase_id=phase_id,
            role_dir=role_dir,
            sub_role=SubRole.EXECUTOR,
            model=step_config.model,
            prompt=prompt,
            task_context=task_context,
            workspace_root=workspace_root,
            metadata=metadata,
        )
    )
