#!/usr/bin/env python3
"""Inspect persisted runtime step state for one task-unit attempt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from workflow_runtime.integrations.tasks_storage import read_runtime_step_summary, resolve_task_directory


DEFAULT_SUBTASK_ID = "phase-level"


def _normalize_subtask_id(subtask_id: str | None) -> str:
    normalized = str(subtask_id or "").strip()
    return normalized or DEFAULT_SUBTASK_ID


def _resolve_attempt_directory(
    *,
    task_id: str,
    phase_id: str,
    subtask_id: str | None,
    sub_role: str,
    attempt: int | None,
) -> Path:
    task_dir = resolve_task_directory(task_id)
    role_dir = (
        task_dir
        / "runtime_artifacts"
        / "step_payloads"
        / phase_id
        / _normalize_subtask_id(subtask_id)
        / sub_role
    )
    if not role_dir.exists():
        raise FileNotFoundError(f"Persisted step directory not found: {role_dir}")
    if attempt is not None:
        attempt_dir = role_dir / f"attempt-{attempt:03d}"
        if not attempt_dir.exists():
            raise FileNotFoundError(f"Persisted attempt directory not found: {attempt_dir}")
        return attempt_dir
    attempts = sorted(path for path in role_dir.iterdir() if path.is_dir() and path.name.startswith("attempt-"))
    if not attempts:
        raise FileNotFoundError(f"No persisted attempts found under: {role_dir}")
    return attempts[-1]


def load_step_state(
    *,
    task_id: str,
    phase_id: str,
    subtask_id: str | None,
    sub_role: str,
    attempt: int | None = None,
    include_artifacts: bool = False,
) -> dict[str, Any]:
    attempt_dir = _resolve_attempt_directory(
        task_id=task_id,
        phase_id=phase_id,
        subtask_id=subtask_id,
        sub_role=sub_role,
        attempt=attempt,
    )
    summary_path = attempt_dir / "step_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Step summary not found: {summary_path}")
    summary = read_runtime_step_summary(str(summary_path))
    result: dict[str, Any] = {
        "task_id": task_id,
        "phase_id": phase_id,
        "subtask_id": _normalize_subtask_id(subtask_id),
        "sub_role": sub_role,
        "attempt_dir": str(attempt_dir),
        "summary": summary,
    }
    if not include_artifacts:
        return result

    artifacts: dict[str, Any] = {}
    for ref in summary.get("artifact_refs", []):
        if not isinstance(ref, dict):
            continue
        artifact_kind = str(ref.get("artifact_kind") or "").strip()
        artifact_path = str(ref.get("path") or "").strip()
        if not artifact_kind or not artifact_path:
            continue
        path = Path(artifact_path)
        if not path.exists():
            artifacts[artifact_kind] = {"path": artifact_path, "missing": True}
            continue
        if path.suffix == ".json":
            try:
                content: Any = json.loads(path.read_text())
            except json.JSONDecodeError:
                content = path.read_text()
        else:
            content = path.read_text()
        artifacts[artifact_kind] = {"path": artifact_path, "content": content}
    result["artifacts"] = artifacts
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Show persisted runtime step state.")
    parser.add_argument("task_id", help="Task id under task-history")
    parser.add_argument("phase_id", help="Phase id, for example execute or validate")
    parser.add_argument("sub_role", help="Sub-role, for example executor reviewer tester")
    parser.add_argument("--subtask", default=DEFAULT_SUBTASK_ID, help="Subtask id, defaults to phase-level")
    parser.add_argument("--attempt", type=int, default=None, help="Specific attempt number, defaults to latest")
    parser.add_argument(
        "--include-artifacts",
        action="store_true",
        help="Inline linked artifact file contents in the output",
    )
    args = parser.parse_args()

    payload = load_step_state(
        task_id=args.task_id,
        phase_id=args.phase_id,
        subtask_id=args.subtask,
        sub_role=args.sub_role,
        attempt=args.attempt,
        include_artifacts=args.include_artifacts,
    )
    print(json.dumps(payload, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
