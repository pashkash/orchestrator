#!/usr/bin/env python3
"""Entry point: create task folder, bootstrap TASK.md, and run the pipeline.

Each phase agent receives its role prompt + Runtime Task Context with paths.
"""

from __future__ import annotations

import argparse
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
    get_methodology_root_host,
    get_runtime_config,
)
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.integrations.tasks_storage import (
    bootstrap_task_card,
    resolve_openhands_conversations_directory,
    resolve_task_card,
    resolve_task_directory,
)
from workflow_runtime.integrations.task_worktree import prepare_task_worktree
from workflow_runtime.integrations.task_worktree import prepare_task_methodology_docs

logger = get_logger(__name__)

LAMINAR_PROJECT_API_KEY = os.getenv("LAMINAR_API_KEY", "lmnr-proj-squadder-orch-001")

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


@observe(name="pipeline_run")
def run(user_request: str, workspace_root: str | None = None) -> dict:
    runtime_config = get_runtime_config()
    ws = workspace_root or runtime_config.workspace_root_default
    task_id = _generate_task_id(user_request)
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
        user_request=user_request,
        workspace_root=ws,
        task_worktree_root=str(task_worktree),
        task_dir_path=str(task_dir),
        task_card_path=str(resolve_task_card(task_id)),
    )
    resolve_openhands_conversations_directory(task_id).mkdir(parents=True, exist_ok=True)

    logger.info("[RunPipeline][run] task_id=%s | task_dir=%s", task_id, task_dir)

    initial_state: PipelineState = {
        "task_id": task_id,
        "user_request": user_request,
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

    graph = compile_graph(driver_mode=DriverMode.OPENHANDS)
    result = graph.invoke(initial_state, {"recursion_limit": 50})

    final_status = result.get("current_status", "UNKNOWN")
    logger.info(
        "[RunPipeline][run] Pipeline finished. task_id=%s, final_status=%s",
        task_id,
        final_status,
    )
    return result


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


if __name__ == "__main__":
    main()
