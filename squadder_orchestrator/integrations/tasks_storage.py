"""Helpers for locating task artifacts and serializing structured outputs."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import yaml

from squadder_orchestrator.graph_compiler.state_schema import StructuredOutput
from squadder_orchestrator.integrations.phase_config_loader import get_runtime_config


def get_tasks_root() -> Path:
    runtime = get_runtime_config()
    return Path(runtime.tasks_root_default)


def resolve_task_directory(task_id: str) -> Path:
    return get_tasks_root() / task_id


def resolve_task_card(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "TASK.md"


def resolve_subtask_card(task_id: str, subtask_id: str) -> Path:
    return resolve_task_directory(task_id) / f"{subtask_id}.md"


def serialize_structured_output(output: StructuredOutput) -> str:
    return yaml.safe_dump(asdict(output), sort_keys=False, allow_unicode=False)
