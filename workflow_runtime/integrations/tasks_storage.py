"""Helpers for locating task artifacts and serializing structured outputs."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import yaml

from workflow_runtime.graph_compiler.state_schema import StructuredOutput
from workflow_runtime.integrations.phase_config_loader import get_runtime_config


# SEM_BEGIN orchestrator_v1.tasks_storage.get_tasks_root:v1
# type: METHOD
# use_case: Resolves the configured root directory for task artifacts.
# feature:
#   - Runtime code must derive task-card paths from runtime config instead of hardcoded locations
#   - Task card 2026-03-24_1800__multi-agent-system-design, D1-D7
# pre:
#   -
# post:
#   - returns the filesystem root for task artifacts
# invariant:
#   - runtime config remains the source of truth for task storage paths
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - get_runtime_config
# sft: resolve the configured tasks root path for orchestrator task artifacts
# idempotent: true
# logs: -
def get_tasks_root() -> Path:
    runtime = get_runtime_config()
    return Path(runtime.tasks_root_default)


# SEM_END orchestrator_v1.tasks_storage.get_tasks_root:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_task_directory:v1
# type: METHOD
# use_case: Resolves the directory path for one task id under the configured tasks root.
# feature:
#   - Runtime helpers derive task and subtask artifact locations from one consistent directory layout
# pre:
#   - task_id is not empty
# post:
#   - returns the filesystem directory path for that task
# invariant:
#   - task root resolution remains config-driven
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - get_tasks_root
# sft: resolve task directory path from configured tasks root and task id
# idempotent: true
# logs: -
def resolve_task_directory(task_id: str) -> Path:
    return get_tasks_root() / task_id


# SEM_END orchestrator_v1.tasks_storage.resolve_task_directory:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_task_card:v1
# type: METHOD
# use_case: Resolves the main TASK.md path for one task id.
# feature:
#   - Runtime integrations and human review flows need a stable location for the parent task card
# pre:
#   - task_id is not empty
# post:
#   - returns the TASK.md path for the task
# invariant:
#   - task directory layout stays consistent with task-management conventions
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_task_directory
# sft: resolve the parent task card path for one task id
# idempotent: true
# logs: -
def resolve_task_card(task_id: str) -> Path:
    return resolve_task_directory(task_id) / "TASK.md"


# SEM_END orchestrator_v1.tasks_storage.resolve_task_card:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.resolve_subtask_card:v1
# type: METHOD
# use_case: Resolves the markdown artifact path for one subtask under a task directory.
# feature:
#   - Runtime workers and reviewers address subtask artifacts by stable task/subtask identifiers
# pre:
#   - task_id and subtask_id are not empty
# post:
#   - returns the subtask markdown path
# invariant:
#   - subtask files remain colocated under the parent task directory
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - resolve_task_directory
# sft: resolve subtask markdown artifact path from task id and subtask id
# idempotent: true
# logs: -
def resolve_subtask_card(task_id: str, subtask_id: str) -> Path:
    return resolve_task_directory(task_id) / f"{subtask_id}.md"


# SEM_END orchestrator_v1.tasks_storage.resolve_subtask_card:v1


# SEM_BEGIN orchestrator_v1.tasks_storage.serialize_structured_output:v1
# type: METHOD
# use_case: Serializes a typed StructuredOutput into YAML for task artifacts and review flows.
# feature:
#   - StructuredOutput must stay portable across task cards validation and human review steps
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D7
# pre:
#   - output is a valid StructuredOutput dataclass
# post:
#   - returns YAML text with stable field order
# invariant:
#   - output object is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - yaml.YAMLError: serialization failed
# depends:
#   - asdict
#   - yaml.safe_dump
# sft: serialize structured output dataclass into YAML for task artifacts
# idempotent: true
# logs: -
def serialize_structured_output(output: StructuredOutput) -> str:
    return yaml.safe_dump(asdict(output), sort_keys=False, allow_unicode=False)


# SEM_END orchestrator_v1.tasks_storage.serialize_structured_output:v1
