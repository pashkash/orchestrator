"""LangGraph builder for the V1 phase-driven orchestrator."""

from __future__ import annotations

import json
import os
from typing import Any

from langgraph.graph import END, START, StateGraph
from lmnr import Laminar, observe

from workflow_runtime.agent_drivers import (
    DirectLlmDriver,
    LangChainToolsDriver,
    MockDriver,
    OpenHandsDriver,
    RoutingDriver,
)
from workflow_runtime.agent_drivers.base_driver import BaseDriver
from workflow_runtime.graph_compiler.edge_evaluators import collect_phase_targets, resolve_next_phase
from workflow_runtime.graph_compiler.state_schema import DriverMode, ExecutionBackend, PipelineState
from workflow_runtime.graph_compiler.yaml_manifest_parser import FlowManifest, RuntimeConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.openhands_http_api import OpenHandsHttpApi
from workflow_runtime.integrations.phase_config_loader import get_flow_manifest, get_runtime_config
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.node_implementations.human_gate import run_human_gate
from workflow_runtime.node_implementations.phases.collect_phase import run_collect_phase
from workflow_runtime.node_implementations.phases.execute_phase import run_execute_phase
from workflow_runtime.node_implementations.phases.plan_phase import run_plan_phase
from workflow_runtime.node_implementations.phases.validate_phase import run_validate_phase
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner


logger = get_logger(__name__)


def _resolve_driver_mode(*, driver_mode: DriverMode | str | None, trace_id: str) -> DriverMode:
    raw_driver_mode = str(driver_mode or os.getenv("WORKFLOW_RUNTIME_DRIVER_MODE", DriverMode.MOCK)).strip()
    resolved_driver_mode = DriverMode.from_raw(raw_driver_mode)
    if raw_driver_mode.lower() == "openhands":
        logger.warning(
            "[LangGraphBuilder][_resolve_driver_mode][DecisionPoint] trace_id=%s | "
            "Branch: deprecated_alias. Reason: raw_driver_mode=openhands maps to driver_mode=live",
            trace_id,
        )
    return resolved_driver_mode


# SEM_BEGIN orchestrator_v1.langgraph_builder.extract_graph_structure:v1
# type: METHOD
# use_case: Сериализует compiled LangGraph topology в span attributes для Laminar UI.
# feature:
#   - compile/runtime spans должны содержать nodes/edges, иначе UI не может отрисовать graph structure
#   - Laminar auto-instrumentation не материализует graph attrs в нужный span автоматически
# pre:
#   - graph exposes get_graph()
# post:
#   - returns langgraph.nodes and langgraph.edges JSON attributes when extraction succeeds
# invariant:
#   - compiled graph topology is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - CompiledStateGraph.get_graph
# sft: serialize compiled langgraph nodes and edges into span attributes
# idempotent: true
# logs: query: LangGraphBuilder _extract_graph_structure
def _extract_graph_structure(*, graph, trace_id: str, owner_method: str) -> dict[str, str]:
    try:
        compiled_graph = graph.get_graph()
        nodes = [
            {"id": node.id, "name": node.name, "metadata": node.metadata}
            for node in compiled_graph.nodes.values()
        ]
        edges = [
            {"source": edge.source, "target": edge.target, "conditional": edge.conditional}
            for edge in compiled_graph.edges
        ]
        attributes = {
            "langgraph.nodes": json.dumps(nodes),
            "langgraph.edges": json.dumps(edges),
        }
        logger.info(
            "[LangGraphBuilder][_extract_graph_structure][StepComplete] trace_id=%s | "
            "Graph attributes extracted. owner_method=%s, nodes=%d, edges=%d",
            trace_id,
            owner_method,
            len(nodes),
            len(edges),
        )
        return attributes
    except Exception:
        logger.error(
            "[LangGraphBuilder][_extract_graph_structure][ErrorHandled][ERR:UNEXPECTED] trace_id=%s | "
            "Graph attribute extraction failed. owner_method=%s",
            trace_id,
            owner_method,
            exc_info=True,
        )
        return {}
# SEM_END orchestrator_v1.langgraph_builder.extract_graph_structure:v1


def _configured_execution_backends(runtime_config: RuntimeConfig) -> set[ExecutionBackend]:
    backends: set[ExecutionBackend] = set()
    for phase_config in runtime_config.phases.values():
        pipelines = [phase_config.pipeline, phase_config.default_worker_pipeline]
        for pipeline in pipelines:
            if pipeline is None:
                continue
            steps = [pipeline.executor, pipeline.reviewer, pipeline.tester]
            for step in steps:
                if step is None:
                    continue
                backends.add(step.execution.backend)
    return backends


# SEM_BEGIN orchestrator_v1.langgraph_builder.build_driver:v1
# type: METHOD
# use_case: Builds the runtime driver for graph compilation from the selected mode and runtime config.
# feature:
#   - The same phase graph can run on top of mock or OpenHands runtime
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   - driver_mode is supported
#   - For live mode, base_url resolves from env or runtime config default
# post:
#   - returns a ready BaseDriver
# invariant:
#   - graph contract does not change due to driver implementation
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: pre[0] violated
# depends:
#   - MockDriver
#   - OpenHandsDriver
# sft: build the runtime driver for the compiled phase graph from the selected mode and runtime config
# idempotent: true
# logs: query: LangGraphBuilder build_driver
def _build_driver(driver_mode: DriverMode | str, runtime_config: RuntimeConfig) -> BaseDriver:
    trace_id = ensure_trace_id()
    resolved_driver_mode = DriverMode.from_raw(driver_mode)
    if str(driver_mode).strip().lower() == "openhands":
        logger.warning(
            "[LangGraphBuilder][_build_driver][DecisionPoint] trace_id=%s | "
            "Branch: deprecated_alias. Reason: driver_mode=openhands maps to driver_mode=live",
            trace_id,
        )
    logger.info(
        "[LangGraphBuilder][_build_driver][ContextAnchor] trace_id=%s | "
        "Building runtime driver. driver_mode=%s",
        trace_id,
        resolved_driver_mode,
    )
    if resolved_driver_mode == DriverMode.MOCK:
        logger.info(
            "[LangGraphBuilder][_build_driver][DecisionPoint] trace_id=%s | "
            "Branch: mock_driver. Reason: driver_mode=mock",
            trace_id,
        )
        driver = MockDriver()
        logger.info(
            "[LangGraphBuilder][_build_driver][StepComplete] trace_id=%s | "
            "Built runtime driver. driver_class=%s",
            trace_id,
            driver.__class__.__name__,
        )
        return driver
    if resolved_driver_mode != DriverMode.LIVE:
        logger.error(
            "[LangGraphBuilder][_build_driver][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Unsupported driver mode. driver_mode=%s",
            trace_id,
            resolved_driver_mode,
        )
        raise ValueError(f"Unsupported driver mode: {resolved_driver_mode}")

    openhands_config = runtime_config.openhands
    direct_llm_config = runtime_config.direct_llm or {}
    tool_agent_config = runtime_config.langchain_tools or {}

    base_url_env = openhands_config["base_url_env"]
    llm_api_key_env = openhands_config["llm_api_key_env"]
    base_url = os.getenv(base_url_env) or str(openhands_config.get("base_url_default", "")).strip()
    llm_api_key = os.getenv(llm_api_key_env)
    direct_llm_api_key = os.getenv(str(direct_llm_config.get("llm_api_key_env", llm_api_key_env)))
    tool_agent_api_key = os.getenv(str(tool_agent_config.get("llm_api_key_env", llm_api_key_env)))
    configured_backends = _configured_execution_backends(runtime_config)
    logger.info(
        "[LangGraphBuilder][_build_driver][PreCheck] trace_id=%s | "
        "Checking OpenHands runtime config. base_url_env=%s, llm_api_key_env=%s",
        trace_id,
        base_url_env,
        llm_api_key_env,
    )
    if not base_url:
        logger.error(
            "[LangGraphBuilder][_build_driver][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Missing OpenHands base URL env var. env=%s",
            trace_id,
            base_url_env,
        )
        raise ValueError(f"Missing OpenHands base URL: env {base_url_env} or runtime.openhands.base_url_default")
    if ExecutionBackend.DIRECT_LLM in configured_backends and not direct_llm_api_key:
        direct_llm_api_key_env = str(direct_llm_config.get("llm_api_key_env", llm_api_key_env))
        raise ValueError(f"Missing direct LLM API key: env {direct_llm_api_key_env}")
    if ExecutionBackend.LANGCHAIN_TOOLS in configured_backends and not tool_agent_api_key:
        tool_api_key_env = str(tool_agent_config.get("llm_api_key_env", llm_api_key_env))
        raise ValueError(f"Missing LangChain tools API key: env {tool_api_key_env}")

    api = OpenHandsHttpApi(
        base_url=base_url,
        timeout_seconds=int(openhands_config["timeout_seconds"]),
        poll_interval_seconds=int(openhands_config["poll_interval_seconds"]),
        max_poll_interval_seconds=int(openhands_config["max_poll_interval_seconds"]),
        poll_log_every_n_attempts=int(openhands_config["poll_log_every_n_attempts"]),
    )
    openhands_driver = OpenHandsDriver(
        api=api,
        llm_api_key=llm_api_key,
        llm_base_url=str(openhands_config["llm_base_url"]),
        cli_mode=bool(openhands_config["cli_mode"]),
        tools=list(openhands_config.get("tools", [])),
    )
    direct_llm_driver = DirectLlmDriver(
        llm_api_key=direct_llm_api_key,
        llm_base_url=str(direct_llm_config.get("llm_base_url", openhands_config["llm_base_url"])),
        timeout_seconds=int(direct_llm_config.get("timeout_seconds", openhands_config["timeout_seconds"])),
        idle_timeout_seconds=int(direct_llm_config.get("idle_timeout_seconds", 15)),
        max_attempts=int(direct_llm_config["max_attempts"]),
        retry_backoff_seconds=int(direct_llm_config["retry_backoff_seconds"]),
    )
    langchain_tools_driver = LangChainToolsDriver(
        llm_api_key=tool_agent_api_key,
        llm_base_url=str(tool_agent_config.get("llm_base_url", openhands_config["llm_base_url"])),
        timeout_seconds=int(tool_agent_config.get("timeout_seconds", openhands_config["timeout_seconds"])),
        max_iterations=int(tool_agent_config.get("max_iterations", 8)),
        shell_timeout_seconds=int(tool_agent_config.get("shell_timeout_seconds", 20)),
        max_output_chars=int(tool_agent_config.get("max_output_chars", 12000)),
    )
    driver = RoutingDriver(
        backends={
            ExecutionBackend.OPENHANDS: openhands_driver,
            ExecutionBackend.DIRECT_LLM: direct_llm_driver,
            ExecutionBackend.LANGCHAIN_TOOLS: langchain_tools_driver,
        }
    )
    logger.info(
        "[LangGraphBuilder][_build_driver][StepComplete] trace_id=%s | "
        "Built runtime driver. driver_class=%s",
        trace_id,
        driver.__class__.__name__,
    )
    return driver


# SEM_END orchestrator_v1.langgraph_builder.build_driver:v1


# SEM_BEGIN orchestrator_v1.langgraph_builder.phase_router:v1
# type: METHOD
# use_case: Builds one conditional-edge router closure for a specific phase id.
# feature:
#   - LangGraph conditional edges need a callable that translates manifest status routing into node targets
# pre:
#   - phase_id exists in the loaded manifest
# post:
#   - returns a router callable that maps state to the next phase or END
# invariant:
#   - closure reuses the same manifest without mutating it
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - KeyError: no transition exists for the current phase/status pair
# depends:
#   - resolve_next_phase
# sft: build a conditional-edge router closure for one phase using the loaded flow manifest
# idempotent: true
# logs: query: LangGraphBuilder _phase_router
def _phase_router(phase_id: str, manifest: FlowManifest):
    def route(state: PipelineState) -> str:
        target = resolve_next_phase(phase_id, state, manifest)
        return END if target == manifest.end_phase else target

    return route


# SEM_END orchestrator_v1.langgraph_builder.phase_router:v1


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
@observe(name="langgraph_compile_graph")
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
    selected_driver_mode = _resolve_driver_mode(
        driver_mode=driver_mode,
        trace_id=resolved_trace_id,
    )
    selected_driver = driver or _build_driver(selected_driver_mode, loaded_runtime_config)
    task_unit_runner = TaskUnitRunner(selected_driver)

    logger.info(
        "[LangGraphBuilder][compile_graph][ContextAnchor] trace_id=%s | "
        "Compiling V1 graph. driver_mode=%s",
        resolved_trace_id,
        selected_driver_mode,
    )
    Laminar.set_span_attributes(
        {
            "langgraph.start_phase": loaded_flow_manifest.start_phase,
            "langgraph.end_phase": loaded_flow_manifest.end_phase,
            "langgraph.phase_ids": [str(phase.id) for phase in loaded_flow_manifest.phases],
            "langgraph.driver_mode": str(selected_driver_mode),
        }
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
    Laminar.set_span_attributes(
        _extract_graph_structure(
            graph=graph,
            trace_id=resolved_trace_id,
            owner_method="compile_graph",
        )
    )
    logger.info(
        "[LangGraphBuilder][compile_graph][StepComplete] trace_id=%s | "
        "Compiled V1 graph. phases=%d",
        resolved_trace_id,
        len(loaded_flow_manifest.phases),
    )
    Laminar.set_span_output(
        {
            "start_phase": loaded_flow_manifest.start_phase,
            "end_phase": loaded_flow_manifest.end_phase,
            "phase_ids": [str(phase.id) for phase in loaded_flow_manifest.phases],
        }
    )
    return graph


# SEM_END orchestrator_v1.langgraph_builder.compile_graph:v1
