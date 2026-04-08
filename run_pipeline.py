#!/usr/bin/env python3
"""Entry point: create task folder, bootstrap TASK.md, and run the pipeline.

Each phase agent receives its role prompt + Runtime Task Context with paths.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime

from lmnr import Laminar, observe

from workflow_runtime.graph_compiler.langgraph_builder import compile_graph
from workflow_runtime.graph_compiler.state_schema import (
    DriverMode,
    PipelineState,
    PipelineStatus,
)
from workflow_runtime.integrations.phase_config_loader import (
    build_role_workspace_repo_map,
    get_methodology_root_host,
    get_runtime_config,
    get_task_repositories,
)
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.integrations.tasks_storage import (
    resolve_openhands_conversations_directory,
    resolve_task_card,
    resolve_task_directory,
)
from workflow_runtime.integrations.task_worktree import prepare_task_workspace_repositories
from workflow_runtime.integrations.task_worktree import prepare_task_methodology_docs

logger = get_logger(__name__)

LAMINAR_PROJECT_API_KEY = os.getenv("LAMINAR_API_KEY", "lmnr-proj-squadder-orch-001")

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str, max_len: int = 60) -> str:
    slug = _SLUG_RE.sub("-", text.lower()).strip("-")
    return slug[:max_len].rstrip("-")


# SEM_BEGIN orchestrator_v1.run_pipeline.ordered_task_repositories:v1
# type: METHOD
# use_case: Определяет порядок task repositories для конкретного запуска, чтобы выбранный workspace стал primary repo.
# feature:
#   - Multi-repo runtime создаёт worktree для всех configured repositories, но первый repo становится primary context для pipeline run
#   - CLI flag --workspace должен влиять на primary_workspace_repo_id без изменения runtime.task_repositories конфигурации
# pre:
#   - workspace_root is None or absolute repo root candidate
# post:
#   - returns repositories in configured order or with the matching workspace repo moved to the front
# invariant:
#   - repository membership does not change, only ordering can change
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_task_repositories
# sft: order configured task repositories so the requested workspace becomes the primary runtime repo
# idempotent: true
# logs: query: RunPipeline _ordered_task_repositories

def _ordered_task_repositories(*, workspace_root: str | None = None):
    repositories = list(get_task_repositories())
    if not workspace_root:
        return repositories
    normalized = workspace_root.rstrip("/")
    matching = [repo for repo in repositories if repo.source_repo_root.rstrip("/") == normalized]
    if not matching:
        return repositories
    leading = matching[0]
    return [leading, *[repo for repo in repositories if repo.id != leading.id]]
# SEM_END orchestrator_v1.run_pipeline.ordered_task_repositories:v1


# SEM_BEGIN orchestrator_v1.run_pipeline.extract_graph_structure:v1
# type: METHOD
# use_case: Достаёт topology compiled orchestrator graph и готовит Laminar span attrs для верхнеуровневого workflow span.
# feature:
#   - langgraph_orchestrator_workflow должен содержать nodes/edges, иначе Laminar UI не показывает граф верхнего pipeline
#   - observability fallback допустим только с явным error log, без silent swallow
# pre:
#   - graph exposes get_graph()
# post:
#   - returns langgraph.nodes/langgraph.edges JSON attrs or empty dict when observability extraction failed
# invariant:
#   - runtime graph instance is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - CompiledStateGraph.get_graph
# sft: serialize orchestrator graph structure into laminar span attributes
# idempotent: true
# logs: query: run_pipeline _extract_graph_structure
def _extract_graph_structure(*, graph, trace_id: str) -> dict[str, str]:
    try:
        g = graph.get_graph()
        nodes = [{"id": n.id, "name": n.name, "metadata": n.metadata} for n in g.nodes.values()]
        edges = [
            {"source": e.source, "target": e.target, "conditional": e.conditional}
            for e in g.edges
        ]
        attributes = {
            "langgraph.nodes": json.dumps(nodes),
            "langgraph.edges": json.dumps(edges),
        }
        logger.info(
            "[RunPipeline][_extract_graph_structure][StepComplete] trace_id=%s | "
            "Orchestrator graph attributes extracted. nodes=%d, edges=%d",
            trace_id,
            len(nodes),
            len(edges),
        )
        return attributes
    except Exception:
        logger.error(
            "[RunPipeline][_extract_graph_structure][ErrorHandled][ERR:UNEXPECTED] trace_id=%s | "
            "Failed to extract orchestrator graph attributes.",
            trace_id,
            exc_info=True,
        )
        return {}
# SEM_END orchestrator_v1.run_pipeline.extract_graph_structure:v1


# SEM_BEGIN orchestrator_v1.run_pipeline.invoke_compiled_graph:v1
# type: METHOD
# use_case: Запускает скомпилированный top-level LangGraph и добавляет pipeline/graph attrs в workflow span.
# feature:
#   - Laminar UI ожидает pipeline metadata и graph JSON на invoke span для визуализации текущего run
#   - runtime должен вызывать graph через единый traced wrapper, а не напрямую из run()
# pre:
#   - initial_state contains task_id and current_phase
#   - graph is already compiled
# post:
#   - current workflow span has pipeline attrs and graph attrs before graph.invoke()
#   - returns final graph state from LangGraph
# invariant:
#   - initial_state is passed into graph.invoke unchanged by this wrapper
# modifies (internal):
#   -
# emits (external):
#   - external.langgraph
# errors:
#   - -
# depends:
#   - Laminar
#   - _extract_graph_structure
# sft: invoke the compiled orchestrator graph through a traced wrapper with pipeline and graph span attributes
# idempotent: false
# logs: query: RunPipeline _invoke_compiled_graph
@observe(name="langgraph_orchestrator_workflow")
def _invoke_compiled_graph(*, graph, initial_state: PipelineState) -> dict:
    trace_id = str(initial_state.get("trace_id") or "")
    logger.info(
        "[RunPipeline][_invoke_compiled_graph][ContextAnchor] trace_id=%s | "
        "Invoking compiled orchestrator graph. task_id=%s, start_phase=%s, primary_workspace_repo_id=%s",
        trace_id,
        initial_state["task_id"],
        initial_state["current_phase"],
        initial_state.get("primary_workspace_repo_id", ""),
    )
    Laminar.set_span_attributes(
        {
            "pipeline.task_id": initial_state["task_id"],
            "pipeline.start_phase": initial_state["current_phase"],
            "pipeline.primary_workspace_repo_id": initial_state.get("primary_workspace_repo_id", ""),
            "pipeline.task_workspace_repo_ids": sorted(initial_state.get("task_workspace_repos", {}).keys()),
            **_extract_graph_structure(graph=graph, trace_id=trace_id),
        }
    )
    result = graph.invoke(initial_state, {"recursion_limit": 50})
    logger.info(
        "[RunPipeline][_invoke_compiled_graph][StepComplete] trace_id=%s | "
        "Compiled orchestrator graph finished. task_id=%s, final_phase=%s, final_status=%s",
        trace_id,
        initial_state["task_id"],
        result.get("current_phase", ""),
        result.get("current_status", ""),
    )
    return result
# SEM_END orchestrator_v1.run_pipeline.invoke_compiled_graph:v1


# SEM_BEGIN orchestrator_v1.run_pipeline.generate_task_id:v1
# type: METHOD
# use_case: Генерирует stable-looking task id из текущего времени и slug user request.
# feature:
#   - task-history и worktree branch names используют единый task_id contract
# pre:
#   - user_request may be empty but string-like
# post:
#   - returns timestamp__slug task id
# invariant:
#   - task_id always starts with UTC timestamp prefix
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _slugify
# sft: generate a task id from the current utc timestamp and the user request slug
# idempotent: false
# logs: -
def _generate_task_id(user_request: str) -> str:
    ts = datetime.now(UTC).strftime("%Y-%m-%d_%H%M")
    slug = _slugify(user_request)
    return f"{ts}__{slug}"
# SEM_END orchestrator_v1.run_pipeline.generate_task_id:v1


# SEM_BEGIN orchestrator_v1.run_pipeline.run:v1
# type: METHOD
# use_case: Создаёт task-local runtime окружение и запускает orchestrator pipeline для одного user request.
# feature:
#   - Каждый запуск должен получить task dir, multi-repo workspace, projected docs, task artifacts и initial PipelineState
#   - Production path всегда компилирует LIVE-mode graph, который внутри dispatch-ит hybrid RoutingDriver
# pre:
#   - user_request is not empty
# post:
#   - task directory and workspace exist for the run
#   - compiled graph executed and final state returned
# invariant:
#   - configured task repositories are not mutated, only ordered for the current run
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.langgraph
# errors:
#   - -
# depends:
#   - prepare_task_workspace_repositories
#   - prepare_task_methodology_docs
#   - compile_graph
# sft: bootstrap task-local orchestrator runtime state and execute the compiled pipeline graph
# idempotent: false
# logs: query: RunPipeline run
@observe(name="pipeline_run")
def run(user_request: str, workspace_root: str | None = None) -> dict:
    runtime_config = get_runtime_config()
    ordered_repositories = _ordered_task_repositories(workspace_root=workspace_root)
    primary_repository = ordered_repositories[0] if ordered_repositories else None
    ws = (
        primary_repository.source_repo_root
        if primary_repository is not None
        else (workspace_root or runtime_config.workspace_root_default)
    )
    task_id = _generate_task_id(user_request)
    task_dir = resolve_task_directory(task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "[RunPipeline][run][ContextAnchor] trace_id=%s | "
        "Bootstrapping pipeline run. task_id=%s, requested_workspace_root=%s, resolved_workspace_root=%s, primary_workspace_repo_id=%s",
        task_id,
        task_id,
        workspace_root or "",
        ws,
        primary_repository.id if primary_repository is not None else "",
    )
    task_workspace_root = task_dir / "workspace"
    task_workspace_repos = prepare_task_workspace_repositories(
        task_id=task_id,
        task_dir_path=str(task_dir),
        repositories=ordered_repositories,
    )
    task_workspace_repo_mapping = {
        repo_id: str(path)
        for repo_id, path in task_workspace_repos.items()
    }
    source_workspace_roots = {
        repository.id: repository.source_repo_root
        for repository in ordered_repositories
    }
    role_workspace_repo_map = build_role_workspace_repo_map()
    task_methodology_docs = prepare_task_methodology_docs(
        task_dir_path=str(task_dir),
        methodology_source_root=str(get_methodology_root_host()),
    )
    task_card_path = resolve_task_card(task_id)
    resolve_openhands_conversations_directory(task_id).mkdir(parents=True, exist_ok=True)

    logger.info("[RunPipeline][run] task_id=%s | task_dir=%s", task_id, task_dir)
    Laminar.set_span_attributes(
        {
            "pipeline.task_id": task_id,
            "pipeline.primary_workspace_repo_id": primary_repository.id if primary_repository is not None else "",
            "pipeline.source_workspace_root": ws,
            "pipeline.task_workspace_repo_ids": sorted(task_workspace_repo_mapping.keys()),
        }
    )

    initial_state: PipelineState = {
        "task_id": task_id,
        "user_request": user_request,
        "workspace_root": ws,
        "primary_workspace_repo_id": primary_repository.id if primary_repository is not None else "",
        "source_workspace_roots": source_workspace_roots,
        "role_workspace_repo_map": role_workspace_repo_map,
        "task_workspace_repos": task_workspace_repo_mapping,
        "task_worktree_root": str(task_workspace_root),
        "methodology_root_host": str(get_methodology_root_host()),
        "methodology_root_runtime": str(task_methodology_docs),
        "methodology_agents_entrypoint": str(task_methodology_docs / "AGENTS.md"),
        "task_dir_path": str(task_dir),
        "task_card_path": str(task_card_path),
        "openhands_conversations_dir": str(resolve_openhands_conversations_directory(task_id)),
        "current_phase": "collect",
        "current_status": PipelineStatus.PASS,
        "phase_attempts": {},
        "current_state": {},
        "plan": [],
        "active_subtask_id": None,
        "structured_outputs": [],
        "merged_summary": {},
        "phase_outputs": {},
        "execution_errors": [],
        "human_decisions": [],
        "runtime_step_refs": [],
        "latest_step_ref_by_key": {},
        "pending_human_input": None,
        "pending_approval_ref": None,
        "human_decision_refs": [],
        "cleanup_manifest_ref": None,
        "final_result": None,
        "commits": [],
        "trace_id": task_id,
    }

    graph = compile_graph(driver_mode=DriverMode.LIVE)
    result = _invoke_compiled_graph(graph=graph, initial_state=initial_state)

    final_status = result.get("current_status", "UNKNOWN")
    logger.info(
        "[RunPipeline][run][StepComplete] trace_id=%s | "
        "Pipeline run finished. task_id=%s, final_status=%s, task_dir=%s",
        task_id,
        task_id,
        final_status,
        task_dir,
    )
    Laminar.set_span_output(
        {
            "task_id": task_id,
            "final_status": final_status,
            "task_dir_path": str(task_dir),
            "task_workspace_repo_ids": sorted(task_workspace_repo_mapping.keys()),
        }
    )
    return result
# SEM_END orchestrator_v1.run_pipeline.run:v1


# SEM_BEGIN orchestrator_v1.run_pipeline.main:v1
# type: METHOD
# use_case: CLI entrypoint для ручного запуска orchestrator pipeline из shell.
# feature:
#   - локальная отладка и ручные верификации должны использовать тот же bootstrap path, что и production entrypoint
# pre:
#   - CLI request argument is provided
# post:
#   - Laminar initialized and run() executed
#   - process exits non-zero when final status is not PASS
# invariant:
#   - CLI argument parsing does not mutate pipeline result
# modifies (internal):
#   -
# emits (external):
#   - external.laminar
# errors:
#   - SystemExit: final pipeline status is not PASS
# depends:
#   - Laminar.initialize
#   - run
# sft: run the orchestrator pipeline from the command line and exit nonzero on failure
# idempotent: false
# logs: query: RunPipeline main
def main() -> None:
    Laminar.initialize(
        project_api_key=LAMINAR_PROJECT_API_KEY,
        base_url="http://localhost",
        http_port=8000,
        grpc_port=8001,
    )

    parser = argparse.ArgumentParser(description="Run the orchestrator pipeline.")
    parser.add_argument("request", help="User request")
    parser.add_argument("--workspace", default=None, help="Override workspace root")
    args = parser.parse_args()

    result = run(args.request, workspace_root=args.workspace)
    final_status = result.get("current_status", "UNKNOWN")
    task_id = result.get("task_id", "?")
    print(f"\n=== Pipeline complete ===")
    print(f"Task ID : {task_id}")
    print(f"Status  : {final_status}")
    print(f"Task dir: {result.get('task_dir_path', '?')}")

    if final_status != str(PipelineStatus.PASS):
        sys.exit(1)
# SEM_END orchestrator_v1.run_pipeline.main:v1


if __name__ == "__main__":
    main()
