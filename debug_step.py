#!/usr/bin/env python3
"""Step-by-step pipeline debugger using LangGraph stream.

Usage:
    uv run python debug_step.py "Your task request"

Streams the graph step by step, printing state after each phase.
Lets you inspect prompts, payloads, and statuses without waiting blindly.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from workflow_runtime.graph_compiler.langgraph_builder import compile_graph
from workflow_runtime.graph_compiler.state_schema import (
    DriverMode,
    PipelineState,
    PipelineStatus,
)
from workflow_runtime.integrations.phase_config_loader import (
    get_methodology_root_host,
    get_runtime_config,
)
from workflow_runtime.integrations.tasks_storage import (
    bootstrap_task_card,
    resolve_openhands_conversations_directory,
    resolve_task_card,
    resolve_task_directory,
)
from workflow_runtime.integrations.task_worktree import prepare_task_worktree
from workflow_runtime.integrations.task_worktree import prepare_task_methodology_docs

import re

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str, max_len: int = 60) -> str:
    slug = _SLUG_RE.sub("-", text.lower()).strip("-")
    return slug[:max_len].rstrip("-")


def _task_sparse_paths(workspace_root: str) -> tuple[str, ...]:
    if workspace_root.rstrip("/") == "/root/squadder-devops":
        return ("orchestrator",)
    return ()


def _generate_task_id(user_request: str) -> str:
    ts = datetime.now(UTC).strftime("%Y-%m-%d_%H%M")
    slug = _slugify(user_request)
    return f"{ts}__{slug}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Step-by-step pipeline debugger.")
    parser.add_argument("request", help="User request")
    parser.add_argument("--workspace", default=None, help="Override workspace root")
    parser.add_argument("--dry-run", action="store_true", help="Use mock driver")
    args = parser.parse_args()

    runtime_config = get_runtime_config()
    ws = args.workspace or runtime_config.workspace_root_default
    task_id = _generate_task_id(args.request)
    task_dir = resolve_task_directory(task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    task_worktree = prepare_task_worktree(
        source_repo_root=ws,
        task_id=task_id,
        task_dir_path=str(task_dir),
        sparse_paths=_task_sparse_paths(ws),
    )
    task_methodology_docs = prepare_task_methodology_docs(
        task_dir_path=str(task_dir),
        methodology_source_root=str(get_methodology_root_host()),
    )
    task_card_path = bootstrap_task_card(
        task_id=task_id,
        user_request=args.request,
        workspace_root=ws,
        task_worktree_root=str(task_worktree),
        task_dir_path=str(task_dir),
        task_card_path=str(resolve_task_card(task_id)),
    )
    resolve_openhands_conversations_directory(task_id).mkdir(parents=True, exist_ok=True)

    driver_mode = DriverMode.MOCK if args.dry_run else DriverMode.OPENHANDS

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
        "task_worktree_root": str(task_worktree),
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
        "pending_human_input": None,
        "final_result": None,
        "commits": [],
    }

    graph = compile_graph(driver_mode=driver_mode)

    step_num = 0
    for event in graph.stream(initial_state, {"recursion_limit": 50}):
        step_num += 1
        for node_name, node_output in event.items():
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


if __name__ == "__main__":
    main()
