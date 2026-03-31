"""Compatibility loaders for the V1 runtime configuration."""

from __future__ import annotations

from pathlib import Path

import yaml

from squadder_orchestrator.graph_compiler.state_schema import PhaseId, SubRole
from squadder_orchestrator.integrations.phase_config_loader import (
    FLOW_MANIFEST_PATH,
    PHASES_RUNTIME_PATH,
    RoleMetadata,
    get_docs_root,
    get_flow_manifest,
    get_runtime_config,
    load_all_role_metadata,
    load_role_metadata,
    resolve_runtime_path,
)
from squadder_orchestrator.integrations.prompt_composer import compose_prompt


DOCS_ROOT = get_docs_root()
ROLES_DIR = DOCS_ROOT / "common" / "roles"
FLOW_PATH = FLOW_MANIFEST_PATH
PHASES_AND_ROLES_PATH = PHASES_RUNTIME_PATH


def load_role(name: str) -> RoleMetadata:
    return load_role_metadata(name)


def load_all_roles() -> dict[str, RoleMetadata]:
    return load_all_role_metadata()


def load_flow_spec(path: Path | None = None) -> dict:
    with (path or FLOW_PATH).open() as handle:
        return yaml.safe_load(handle)


def load_phases_runtime_spec(path: Path | None = None) -> dict:
    with (path or PHASES_AND_ROLES_PATH).open() as handle:
        return yaml.safe_load(handle)


def load_flow(path: Path | None = None):
    if path is not None and path != FLOW_PATH:
        with path.open() as handle:
            return yaml.safe_load(handle)
    return get_flow_manifest()


def load_runtime(path: Path | None = None):
    if path is not None and path != PHASES_AND_ROLES_PATH:
        with path.open() as handle:
            return yaml.safe_load(handle)
    return get_runtime_config()


def build_prompt(
    role_name: str,
    sub_role: str,
    *,
    phase_id: str = "execute",
    task_context: dict | None = None,
) -> str:
    runtime = get_runtime_config()
    resolved_phase = PhaseId(phase_id)
    resolved_sub_role = SubRole(sub_role)
    if resolved_phase == PhaseId.EXECUTE:
        pipeline = runtime.phases["execute"].default_worker_pipeline
    else:
        pipeline = runtime.phases[resolved_phase].pipeline
    step_config = getattr(pipeline, resolved_sub_role)
    return compose_prompt(
        phase_id=resolved_phase,
        role_dir=role_name,
        step_config=step_config,
        task_context=task_context or {},
    )


__all__ = [
    "DOCS_ROOT",
    "FLOW_PATH",
    "PHASES_AND_ROLES_PATH",
    "ROLES_DIR",
    "build_prompt",
    "load_all_roles",
    "load_flow",
    "load_flow_spec",
    "load_phases_runtime_spec",
    "load_role",
    "load_runtime",
    "resolve_runtime_path",
]
