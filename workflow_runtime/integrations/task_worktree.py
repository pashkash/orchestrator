"""Helpers for provisioning task-specific git worktrees and task-local methodology access."""

from __future__ import annotations

import subprocess
from pathlib import Path

from workflow_runtime.graph_compiler.yaml_manifest_parser import TaskRepositoryConfig
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger
from workflow_runtime.integrations.tasks_storage import resolve_task_directory, resolve_task_worktree_directory

logger = get_logger(__name__)


def _run_git_command(*args: str, trace_id: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _apply_sparse_checkout(
    *,
    worktree_dir: Path,
    sparse_paths: tuple[str, ...],
    trace_id: str,
) -> None:
    if not sparse_paths:
        return

    sparse_init = _run_git_command(
        "-C",
        str(worktree_dir),
        "sparse-checkout",
        "init",
        "--cone",
        trace_id=trace_id,
    )
    if sparse_init.returncode != 0:
        raise RuntimeError(
            f"Failed to initialize sparse-checkout for {worktree_dir}: {sparse_init.stderr.strip()}"
        )

    sparse_set = _run_git_command(
        "-C",
        str(worktree_dir),
        "sparse-checkout",
        "set",
        "--cone",
        *sparse_paths,
        trace_id=trace_id,
    )
    if sparse_set.returncode != 0:
        raise RuntimeError(
            f"Failed to set sparse-checkout paths for {worktree_dir}: {sparse_set.stderr.strip()}"
        )


# SEM_BEGIN orchestrator_v1.task_worktree.prepare_task_worktree:v1
# type: METHOD
# use_case: Готовит task-local git worktree для одного source repository.
# feature:
#   - Каждый task получает изолированную git worktree/branch для безопасной работы исполнителей
#   - Existing worktree может быть reused, если это уже корректный git worktree для task branch
# pre:
#   - source_repo_root is a git repository
# post:
#   - returns a ready worktree path for the task branch
# invariant:
#   - source repository history is not rewritten by this method
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - RuntimeError: pre[0] violated
# depends:
#   - _run_git_command
#   - _apply_sparse_checkout
# sft: create or reuse a task-local git worktree for a source repository
# idempotent: false
# logs: query: TaskWorktree prepare_task_worktree
def prepare_task_worktree(
    *,
    source_repo_root: str,
    task_id: str,
    task_dir_path: str | None = None,
    worktree_dir_path: str | None = None,
    branch_prefix: str = "task",
    sparse_paths: tuple[str, ...] | None = None,
) -> Path:
    resolved_trace_id = ensure_trace_id()
    repo_root = Path(source_repo_root).resolve()
    task_dir = Path(task_dir_path).resolve() if task_dir_path else resolve_task_directory(task_id)
    worktree_dir = (
        Path(worktree_dir_path).resolve()
        if worktree_dir_path
        else (
            resolve_task_worktree_directory(task_id)
            if task_dir_path is None
            else task_dir / "workspace"
        )
    )
    branch_name = f"{branch_prefix}/{task_id}"
    resolved_sparse_paths = tuple(path for path in (sparse_paths or ()) if path)

    logger.info(
        "[TaskWorktree][prepare_task_worktree][ContextAnchor] trace_id=%s | "
        "Preparing task worktree. repo_root=%s, task_id=%s, worktree_dir=%s, branch=%s",
        resolved_trace_id,
        repo_root,
        task_id,
        worktree_dir,
        branch_name,
    )

    repo_check = _run_git_command("-C", str(repo_root), "rev-parse", "--show-toplevel", trace_id=resolved_trace_id)
    if repo_check.returncode != 0:
        raise RuntimeError(
            f"Source workspace root is not a git repository: {repo_root}. "
            f"git stderr: {repo_check.stderr.strip()}"
        )

    if worktree_dir.exists() and any(worktree_dir.iterdir()) and not (worktree_dir / ".git").exists():
        raise RuntimeError(
            f"Task worktree directory already exists and is not a git worktree: {worktree_dir}"
        )

    if (worktree_dir / ".git").exists():
        existing_check = _run_git_command(
            "-C",
            str(worktree_dir),
            "rev-parse",
            "--is-inside-work-tree",
            trace_id=resolved_trace_id,
        )
        if existing_check.returncode == 0:
            _apply_sparse_checkout(
                worktree_dir=worktree_dir,
                sparse_paths=resolved_sparse_paths,
                trace_id=resolved_trace_id,
            )
            logger.info(
                "[TaskWorktree][prepare_task_worktree][StepComplete] trace_id=%s | "
                "Reusing existing task worktree. worktree_dir=%s",
                resolved_trace_id,
                worktree_dir,
            )
            return worktree_dir

    worktree_dir.parent.mkdir(parents=True, exist_ok=True)
    branch_exists = (
        _run_git_command(
            "-C",
            str(repo_root),
            "show-ref",
            "--verify",
            "--quiet",
            f"refs/heads/{branch_name}",
            trace_id=resolved_trace_id,
        ).returncode
        == 0
    )

    worktree_add_args = ["-C", str(repo_root), "worktree", "add"]
    if branch_exists:
        worktree_add_args.extend([str(worktree_dir), branch_name])
    else:
        worktree_add_args.extend(["-b", branch_name, str(worktree_dir), "HEAD"])

    worktree_add = _run_git_command(*worktree_add_args, trace_id=resolved_trace_id)
    if worktree_add.returncode != 0:
        raise RuntimeError(
            f"Failed to create task worktree at {worktree_dir} from {repo_root}: "
            f"{worktree_add.stderr.strip()}"
        )

    _apply_sparse_checkout(
        worktree_dir=worktree_dir,
        sparse_paths=resolved_sparse_paths,
        trace_id=resolved_trace_id,
    )

    logger.info(
        "[TaskWorktree][prepare_task_worktree][StepComplete] trace_id=%s | "
        "Task worktree prepared. worktree_dir=%s, branch=%s",
        resolved_trace_id,
        worktree_dir,
        branch_name,
    )
    return worktree_dir
# SEM_END orchestrator_v1.task_worktree.prepare_task_worktree:v1


# SEM_BEGIN orchestrator_v1.task_worktree.prepare_task_workspace_repositories:v1
# type: METHOD
# use_case: Готовит полный multi-repo workspace для task-а по списку configured repositories.
# feature:
#   - orchestration runtime должен развернуть все repo roots, доступные planner/executor steps в одном task workspace
# pre:
#   - repositories list may be empty but must contain valid TaskRepositoryConfig objects when provided
# post:
#   - returns mapping repo_id -> prepared worktree path
# invariant:
#   - repository ids are preserved as workspace folder names
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - prepare_task_worktree
# sft: prepare all configured repository worktrees for a task-local multi-repo workspace
# idempotent: false
# logs: query: TaskWorktree prepare_task_workspace_repositories
def prepare_task_workspace_repositories(
    *,
    task_id: str,
    task_dir_path: str,
    repositories: list[TaskRepositoryConfig],
) -> dict[str, Path]:
    task_dir = Path(task_dir_path).resolve()
    workspace_root = task_dir / "workspace"
    workspace_root.mkdir(parents=True, exist_ok=True)
    repo_worktrees: dict[str, Path] = {}
    for repository in repositories:
        worktree_dir = workspace_root / repository.id
        repo_worktrees[repository.id] = prepare_task_worktree(
            source_repo_root=repository.source_repo_root,
            task_id=task_id,
            task_dir_path=str(task_dir),
            worktree_dir_path=str(worktree_dir),
            branch_prefix=repository.branch_prefix,
            sparse_paths=tuple(repository.default_sparse_paths),
        )
    return repo_worktrees
# SEM_END orchestrator_v1.task_worktree.prepare_task_workspace_repositories:v1


# SEM_BEGIN orchestrator_v1.task_worktree.prepare_task_methodology_docs:v1
# type: METHOD
# use_case: Делает task-local runtime-visible link на methodology/docs root.
# feature:
#   - Tool-capable agents должны видеть methodology packet внутри task directory без копирования всего docs дерева
# pre:
#   - methodology_source_root exists and is a directory
# post:
#   - returns task-local docs symlink path
# invariant:
#   - source docs tree is never modified
# modifies (internal):
#   - file.task_history
# emits (external):
#   -
# errors:
#   - RuntimeError: pre[0] violated
# depends:
#   - ensure_trace_id
# sft: create or reuse a task-local symlink to the methodology docs root
# idempotent: false
# logs: query: TaskWorktree prepare_task_methodology_docs
def prepare_task_methodology_docs(
    *,
    task_dir_path: str,
    methodology_source_root: str,
) -> Path:
    resolved_trace_id = ensure_trace_id()
    task_dir = Path(task_dir_path).resolve()
    docs_target = task_dir / "docs"
    source_root = Path(methodology_source_root).resolve()

    logger.info(
        "[TaskWorktree][prepare_task_methodology_docs][ContextAnchor] trace_id=%s | "
        "Preparing task methodology docs. task_dir=%s, docs_target=%s, source_root=%s",
        resolved_trace_id,
        task_dir,
        docs_target,
        source_root,
    )

    if not source_root.exists() or not source_root.is_dir():
        raise RuntimeError(f"Methodology docs root does not exist: {source_root}")

    if docs_target.is_symlink():
        if docs_target.resolve() == source_root:
            logger.info(
                "[TaskWorktree][prepare_task_methodology_docs][StepComplete] trace_id=%s | "
                "Reusing existing task methodology docs link. docs_target=%s",
                resolved_trace_id,
                docs_target,
            )
            return docs_target
        raise RuntimeError(
            f"Task methodology docs link points to a different target: {docs_target} -> {docs_target.resolve()}"
        )

    if docs_target.exists():
        raise RuntimeError(f"Task methodology docs path already exists and is not a symlink: {docs_target}")

    docs_target.symlink_to(source_root, target_is_directory=True)
    logger.info(
        "[TaskWorktree][prepare_task_methodology_docs][StepComplete] trace_id=%s | "
        "Task methodology docs linked. docs_target=%s",
        resolved_trace_id,
        docs_target,
    )
    return docs_target
# SEM_END orchestrator_v1.task_worktree.prepare_task_methodology_docs:v1
