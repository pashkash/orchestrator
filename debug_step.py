#!/usr/bin/env python3
"""Step-by-step pipeline debugger using LangGraph stream.

Usage:
    uv run python debug_step.py "Your task request"

Streams the graph step by step, printing state after each phase.
Lets you inspect prompts, payloads, and statuses without waiting blindly.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

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
from workflow_runtime.integrations.tasks_storage import (
    resolve_openhands_conversations_directory,
    resolve_task_card,
    resolve_task_directory,
)
from workflow_runtime.integrations.task_worktree import prepare_task_workspace_repositories
from workflow_runtime.integrations.task_worktree import prepare_task_methodology_docs

import re

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str, max_len: int = 60) -> str:
    slug = _SLUG_RE.sub("-", text.lower()).strip("-")
    return slug[:max_len].rstrip("-")


# SEM_BEGIN orchestrator_v1.debug_step.ordered_task_repositories:v1
# type: METHOD
# use_case: Выбирает primary repo порядок для debug run так же, как основной entrypoint.
# feature:
#   - debug CLI должен повторять multi-repo ordering contract run_pipeline.py
# pre:
#   - workspace_root is None or repo root candidate
# post:
#   - returns repositories with the matching workspace repo moved to the front
# invariant:
#   - repository membership does not change
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_task_repositories
# sft: order configured task repositories for the debug entrypoint so the selected workspace becomes primary
# idempotent: true
# logs: -
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
# SEM_END orchestrator_v1.debug_step.ordered_task_repositories:v1


# SEM_BEGIN orchestrator_v1.debug_step.generate_task_id:v1
# type: METHOD
# use_case: Генерирует task id для debug run по тому же формату, что и основной pipeline entrypoint.
# feature:
#   - debug artifacts и runtime artifacts должны использовать тот же naming contract, что и run_pipeline.py
# pre:
#   - user_request may be empty but string-like
# post:
#   - returns timestamp__slug task id
# invariant:
#   - task_id format matches run_pipeline.generate_task_id
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - _slugify
# sft: generate a debug task id using the same timestamp and slug format as the main pipeline entrypoint
# idempotent: false
# logs: -
def _generate_task_id(user_request: str) -> str:
    ts = datetime.now(UTC).strftime("%Y-%m-%d_%H%M")
    slug = _slugify(user_request)
    return f"{ts}__{slug}"
# SEM_END orchestrator_v1.debug_step.generate_task_id:v1


# SEM_BEGIN orchestrator_v1.debug_step.stream_compiled_graph:v1
# type: METHOD
# use_case: Стримит шаги top-level orchestrator graph для интерактивной отладки pipeline run.
# feature:
#   - debug CLI должен показывать phase-by-phase progression без ожидания полного завершения pipeline
# pre:
#   - graph is compiled
#   - initial_state contains pipeline metadata
# post:
#   - returns LangGraph stream iterator with pipeline attrs attached to the current span
# invariant:
#   - initial_state is passed into graph.stream unchanged by this wrapper
# modifies (internal):
#   -
# emits (external):
#   - external.langgraph
# errors:
#   - -
# depends:
#   - Laminar
# sft: stream the compiled orchestrator graph step by step for debugging
# idempotent: false
# logs: -
@observe(name="langgraph_orchestrator_stream")
def _stream_compiled_graph(*, graph, initial_state: PipelineState):
    Laminar.set_span_attributes(
        {
            "pipeline.task_id": initial_state["task_id"],
            "pipeline.start_phase": initial_state["current_phase"],
            "pipeline.primary_workspace_repo_id": initial_state.get("primary_workspace_repo_id", ""),
            "pipeline.task_workspace_repo_ids": sorted(initial_state.get("task_workspace_repos", {}).keys()),
        }
    )
    return graph.stream(initial_state, {"recursion_limit": 50})
# SEM_END orchestrator_v1.debug_step.stream_compiled_graph:v1


# SEM_BEGIN orchestrator_v1.debug_step.main:v1
# type: METHOD
# use_case: CLI entrypoint для пошаговой отладки orchestrator pipeline через streamed graph events.
# feature:
#   - оператор должен видеть phase status, plan, outputs и task artifacts после каждого graph event
# pre:
#   - CLI request argument is provided
# post:
#   - task-local runtime environment bootstrapped and graph stream consumed to completion
# invariant:
#   - debug printing does not mutate graph state
# modifies (internal):
#   - file.task_history
# emits (external):
#   - external.langgraph
# errors:
#   - -
# depends:
#   - compile_graph
#   - _stream_compiled_graph
# sft: run the step-by-step orchestrator debugger and print streamed graph state after each event
# idempotent: false
# logs: -
def main() -> None:
    Laminar.initialize(
        project_api_key="lmnr-proj-squadder-orch-001",
        base_url="http://localhost",
        http_port=8000,
        grpc_port=8001,
    )
    parser = argparse.ArgumentParser(description="Step-by-step pipeline debugger.")
    parser.add_argument("request", help="User request")
    parser.add_argument("--workspace", default=None, help="Override workspace root")
    parser.add_argument("--dry-run", action="store_true", help="Use mock driver")
    args = parser.parse_args()

    runtime_config = get_runtime_config()
    ordered_repositories = _ordered_task_repositories(workspace_root=args.workspace)
    primary_repository = ordered_repositories[0] if ordered_repositories else None
    ws = (
        primary_repository.source_repo_root
        if primary_repository is not None
        else (args.workspace or runtime_config.workspace_root_default)
    )
    task_id = _generate_task_id(args.request)
    task_dir = resolve_task_directory(task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
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

    driver_mode = DriverMode.MOCK if args.dry_run else DriverMode.LIVE

    print(f"\n{'='*60}")
    print(f"Task ID  : {task_id}")
    print(f"Task dir : {task_dir}")
    print(f"Driver   : {driver_mode}")
    print(f"Request  : {args.request}")
    print(f"{'='*60}\n")

    initial_state: PipelineState = {
        "task_id": task_id,
        "user_request": args.request,
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
    }

    graph = compile_graph(driver_mode=driver_mode)

    step_num = 0
    for event in _stream_compiled_graph(graph=graph, initial_state=initial_state):
        step_num += 1
        for node_name, node_output in event.items():
            if not isinstance(node_output, dict):
                print(f"\n{'─'*60}")
                print(f"Step {step_num}: node={node_name}")
                print(f"  raw output type: {type(node_output).__name__}")
                print(f"  raw output: {node_output}")
                continue
            phase = node_output.get("current_phase", "?")
            status = node_output.get("current_status", "?")
            plan_len = len(node_output.get("plan", []))
            outputs_len = len(node_output.get("structured_outputs", []))
            errors = node_output.get("execution_errors", [])
            final = node_output.get("final_result")

            print(f"\n{'─'*60}")
            print(f"Step {step_num}: node={node_name}")
            print(f"  phase  : {phase}")
            print(f"  status : {status}")
            if plan_len:
                plan_items = node_output.get("plan", [])
                print(f"  plan   : {plan_len} items")
                for p in plan_items:
                    pid = getattr(p, 'id', p.get('id', '?') if isinstance(p, dict) else '?')
                    prole = getattr(p, 'role', p.get('role', '?') if isinstance(p, dict) else '?')
                    pstatus = getattr(p, 'status', p.get('status', '?') if isinstance(p, dict) else '?')
                    print(f"    - {pid} ({prole}) [{pstatus}]")
            if outputs_len:
                print(f"  outputs: {outputs_len}")
            if errors:
                print(f"  errors : {errors}")
            if final:
                print(f"  final  : {final}")

            phase_outputs = node_output.get("phase_outputs", {})
            if phase in phase_outputs:
                payload = phase_outputs[phase]
                payload_keys = list(payload.keys()) if isinstance(payload, dict) else str(type(payload))
                print(f"  payload keys: {payload_keys}")

            task_card = Path(initial_state["task_card_path"])
            if task_card.exists():
                print(f"  TASK.md : EXISTS ({task_card.stat().st_size} bytes)")

            workspace = task_dir / "workspace"
            if workspace.exists():
                files = list(workspace.iterdir())
                if files:
                    print(f"  workspace files: {[f.name for f in files]}")

    print(f"\n{'='*60}")
    print(f"Pipeline finished in {step_num} steps")
    print(f"Task dir: {task_dir}")

    task_card = Path(initial_state["task_card_path"])
    if task_card.exists():
        print(f"TASK.md: {task_card}")
    workspace = task_dir / "workspace"
    if workspace.exists():
        files = list(workspace.iterdir())
        print(f"Workspace files: {[f.name for f in files]}")
    print(f"{'='*60}")
# SEM_END orchestrator_v1.debug_step.main:v1


if __name__ == "__main__":
    main()
