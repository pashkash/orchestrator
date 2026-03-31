"""LangGraph builder for the V1 phase-driven orchestrator."""

from __future__ import annotations

import logging
import os
from typing import Any

from langgraph.graph import END, START, StateGraph

from workflow_runtime.agent_drivers import MockDriver, OpenHandsDriver
from workflow_runtime.agent_drivers.base_driver import BaseDriver
from workflow_runtime.graph_compiler.edge_evaluators import collect_phase_targets, resolve_next_phase
from workflow_runtime.graph_compiler.state_schema import DriverMode, PipelineState
from workflow_runtime.graph_compiler.yaml_manifest_parser import FlowManifest, RuntimeConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.openhands_http_api import OpenHandsHttpApi
from workflow_runtime.integrations.phase_config_loader import get_flow_manifest, get_runtime_config
from workflow_runtime.node_implementations.human_gate import run_human_gate
from workflow_runtime.node_implementations.phases.collect_phase import run_collect_phase
from workflow_runtime.node_implementations.phases.execute_phase import run_execute_phase
from workflow_runtime.node_implementations.phases.plan_phase import run_plan_phase
from workflow_runtime.node_implementations.phases.validate_phase import run_validate_phase
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner


logger = logging.getLogger(__name__)


# SEM_BEGIN orchestrator_v1.langgraph_builder.build_driver:v1
# type: METHOD
# use_case: Builds the runtime driver for graph compilation from the selected mode and runtime config.
# feature:
#   - The same phase graph can run on top of mock or OpenHands runtime
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   - driver_mode is supported
#   - For openhands, base_url_env and llm_api_key_env are set
# post:
#   - returns a ready BaseDriver
# invariant:
#   - graph contract does not change due to driver implementation
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: pre[0] or pre[1] violated
# depends:
#   - MockDriver
#   - OpenHandsDriver
# sft: build the runtime driver for the compiled phase graph from the selected mode and runtime config
# idempotent: true
# logs: query: LangGraphBuilder build_driver
def _build_driver(driver_mode: DriverMode, runtime_config: RuntimeConfig) -> BaseDriver:
    if driver_mode == DriverMode.MOCK:
        return MockDriver()
    if driver_mode != DriverMode.OPENHANDS:
        raise ValueError(f"Unsupported driver mode: {driver_mode}")

    base_url_env = runtime_config.openhands["base_url_env"]
    llm_api_key_env = runtime_config.openhands["llm_api_key_env"]
    base_url = os.getenv(base_url_env)
    llm_api_key = os.getenv(llm_api_key_env)
    if not base_url:
        raise ValueError(f"Missing required env var: {base_url_env}")
    if not llm_api_key:
        raise ValueError(f"Missing required env var: {llm_api_key_env}")

    api = OpenHandsHttpApi(
        base_url=base_url,
        timeout_seconds=int(runtime_config.openhands["timeout_seconds"]),
        poll_interval_seconds=int(runtime_config.openhands["poll_interval_seconds"]),
    )
    return OpenHandsDriver(
        api=api,
        llm_api_key=llm_api_key,
        llm_base_url=str(runtime_config.openhands["llm_base_url"]),
        cli_mode=bool(runtime_config.openhands["cli_mode"]),
        tools=list(runtime_config.openhands.get("tools", [])),
    )


# SEM_END orchestrator_v1.langgraph_builder.build_driver:v1


def _phase_router(phase_id: str, manifest: FlowManifest):
    def route(state: PipelineState) -> str:
        target = resolve_next_phase(phase_id, state, manifest)
        return END if target == manifest.end_phase else target

    return route


# SEM_BEGIN orchestrator_v1.langgraph_builder.compile_graph:v1
# type: METHOD
# use_case: Compiles the V1 phase graph from runtime manifests and the selected driver mode.
# feature:
#   - Graph topology is taken from `orchestrator/config/flow.yaml`
#   - phase behaviour is taken from `orchestrator/config/phases_and_roles.yaml`
# pre:
#   - manifests are valid and contain collect/plan/execute/validate/human_gate
# post:
#   - returns compiled LangGraph runnable for V1 pipeline
# invariant:
#   - Python builder does not hardcode business branching except phase wrapper registry
# modifies (internal):
#   - file.orchestrator/config/flow.yaml
#   - file.orchestrator/config/phases_and_roles.yaml
# emits (external):
#   -
# errors:
#   - ValueError: driver mode invalid or required env vars missing
# depends:
#   - StateGraph
#   - TaskUnitRunner
# sft: compile the V1 phase graph from YAML manifests and connect it to the selected driver runtime
# idempotent: true
# logs: command: uv run pytest tests/ -v | query: compiled nodes and selected driver mode
def compile_graph(
    *,
    driver_mode: DriverMode | str | None = None,
    driver: BaseDriver | None = None,
    flow_manifest: FlowManifest | None = None,
    runtime_config: RuntimeConfig | None = None,
    checkpointer: Any | None = None,
):
    resolved_trace_id = ensure_trace_id()
    loaded_flow_manifest = flow_manifest or get_flow_manifest()
    loaded_runtime_config = runtime_config or get_runtime_config()
    selected_driver_mode = DriverMode(
        str(driver_mode or os.getenv("WORKFLOW_RUNTIME_DRIVER_MODE", DriverMode.MOCK))
    )
    selected_driver = driver or _build_driver(selected_driver_mode, loaded_runtime_config)
    task_unit_runner = TaskUnitRunner(selected_driver)

    logger.info(
        "[LangGraphBuilder][compile_graph][ContextAnchor] trace_id=%s | "
        "Compiling V1 graph. driver_mode=%s",
        resolved_trace_id,
        selected_driver_mode,
    )

    builder = StateGraph(PipelineState)
    builder.add_node(
        "collect",
        lambda state: run_collect_phase(
            state,
            task_unit_runner=task_unit_runner,
            phase_config=loaded_runtime_config.phases["collect"],
        ),
    )
    builder.add_node(
        "plan",
        lambda state: run_plan_phase(
            state,
            task_unit_runner=task_unit_runner,
            phase_config=loaded_runtime_config.phases["plan"],
        ),
    )
    builder.add_node(
        "execute",
        lambda state: run_execute_phase(
            state,
            task_unit_runner=task_unit_runner,
            phase_config=loaded_runtime_config.phases["execute"],
        ),
    )
    builder.add_node(
        "validate",
        lambda state: run_validate_phase(
            state,
            task_unit_runner=task_unit_runner,
            phase_config=loaded_runtime_config.phases["validate"],
        ),
    )
    builder.add_node("human_gate", run_human_gate)
    builder.add_edge(START, loaded_flow_manifest.start_phase)

    for phase in loaded_flow_manifest.phases:
        targets = collect_phase_targets(loaded_flow_manifest, phase.id)
        langgraph_targets = [END if target == loaded_flow_manifest.end_phase else target for target in targets]
        builder.add_conditional_edges(
            phase.id,
            _phase_router(phase.id, loaded_flow_manifest),
            langgraph_targets,
        )

    graph = builder.compile(checkpointer=checkpointer)
    logger.info(
        "[LangGraphBuilder][compile_graph][StepComplete] trace_id=%s | "
        "Compiled V1 graph. phases=%d",
        resolved_trace_id,
        len(loaded_flow_manifest.phases),
    )
    return graph


# SEM_END orchestrator_v1.langgraph_builder.compile_graph:v1
