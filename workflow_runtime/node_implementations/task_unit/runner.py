"""Universal TaskUnit runner for collect/plan/execute/validate phases."""

from __future__ import annotations

from typing import Any

from lmnr import observe

from workflow_runtime.agent_drivers.base_driver import BaseDriver
from workflow_runtime.graph_compiler.state_schema import (
    PipelineStatus,
    TaskUnitResult,
)
from workflow_runtime.graph_compiler.yaml_manifest_parser import PipelineConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.node_implementations.task_unit.task_unit_graph import run_task_unit_subgraph


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.task_unit_runner.task_unit_runner:v1
# type: CLASS
# use_case: Thin wrapper that delegates TaskUnit execution to the LangGraph subgraph.
# feature:
#   - V1 standardizes execution around executor reviewer optional tester and simple guardrails
#   - Actual logic lives in task_unit_graph.run_task_unit_subgraph
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
    @observe(name="task_unit_run")
    def run(
        self,
        *,
        phase_id: str,
        role_dir: str,
        pipeline: PipelineConfig,
        task_context: dict[str, Any],
        working_dir: str,
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

        # === PRE[0]: working_dir not empty ===
        logger.info(
            "[TaskUnitRunner][run][PreCheck] trace_id=%s | "
            "Checking working_dir is not empty. phase=%s, role_dir=%s",
            resolved_trace_id,
            phase_id,
            role_dir,
        )
        if not working_dir:
            logger.warning(
                "[TaskUnitRunner][run][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
                "Working dir is empty. phase=%s, role_dir=%s",
                resolved_trace_id,
                phase_id,
                role_dir,
            )
            return TaskUnitResult(
                status=PipelineStatus.BLOCKED,
                warnings=["working_dir is empty"],
            )
        result = run_task_unit_subgraph(
            driver=self._driver,
            phase_id=phase_id,
            role_dir=role_dir,
            pipeline=pipeline,
            task_context=task_context,
            working_dir=working_dir,
            metadata=metadata,
            trace_id=resolved_trace_id,
        )
        logger.info(
            "[TaskUnitRunner][run][StepComplete] trace_id=%s | "
            "Task unit finished via subgraph. phase=%s, role_dir=%s, status=%s",
            resolved_trace_id,
            phase_id,
            role_dir,
            result.status,
        )
        return result

    # SEM_END orchestrator_v1.task_unit_runner.run:v1


# SEM_END orchestrator_v1.task_unit_runner.task_unit_runner:v1
