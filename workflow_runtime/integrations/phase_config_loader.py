"""Cached loaders for V1 orchestrator manifests and role metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

from workflow_runtime.graph_compiler.yaml_manifest_parser import (
    FlowManifest,
    RuntimeConfig,
    load_flow_manifest,
    load_runtime_config,
)
from workflow_runtime.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)

ORCHESTRATOR_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = ORCHESTRATOR_ROOT / "config"
FLOW_MANIFEST_PATH = CONFIG_ROOT / "flow.yaml"
PHASES_RUNTIME_PATH = CONFIG_ROOT / "phases_and_roles.yaml"


@dataclass(frozen=True, slots=True)
class RoleMetadata:
    name: str
    description: str
    guides: list[str] = field(default_factory=list)
    skills: list[str] = field(default_factory=list)
    permissions: dict = field(default_factory=dict)


@lru_cache(maxsize=1)
def get_flow_manifest() -> FlowManifest:
    return load_flow_manifest(FLOW_MANIFEST_PATH)


@lru_cache(maxsize=1)
def get_runtime_config() -> RuntimeConfig:
    return load_runtime_config(PHASES_RUNTIME_PATH)


def get_docs_root() -> Path:
    runtime = get_runtime_config()
    return Path(runtime.docs_root_default)


def resolve_runtime_path(raw_path: str, role_dir: str | None = None) -> Path:
    runtime = get_runtime_config()
    resolved = raw_path.replace("{role_dir}", role_dir or "")
    if resolved.startswith(f"{runtime.docs_root_alias}/"):
        suffix = resolved[len(runtime.docs_root_alias) + 1:]
        return get_docs_root() / suffix
    return Path(resolved)


# SEM_BEGIN orchestrator_v1.phase_config_loader.load_role_metadata:v1
# type: METHOD
# use_case: Loads metadata for a domain or supervisor/collector role from docs role.yaml.
# feature:
#   - Runtime config uses docs/common/roles as the knowledge source
#   - Task card 2026-03-24_1800__multi-agent-system-design, D1
# pre:
#   - role.yaml exists at docs/common/roles/{role_dir}/role.yaml
# post:
#   - returns a typed RoleMetadata
# invariant:
#   - role.yaml is read in readonly mode only
# modifies (internal):
#   - file.docs/common/roles/{role_dir}/role.yaml
# emits (external):
#   -
# errors:
#   - FileNotFoundError: pre[0] violated
# depends:
#   - yaml.safe_load
# sft: load role metadata from docs role.yaml for prompt composition and permissions
# idempotent: true
# logs: path: docs/common/roles/{role_dir}/role.yaml
def load_role_metadata(role_dir: str) -> RoleMetadata:
    trace_id = ensure_trace_id()
    role_yaml_path = get_docs_root() / "common" / "roles" / role_dir / "role.yaml"

    logger.info(
        "[PhaseConfigLoader][load_role_metadata][ContextAnchor] trace_id=%s | "
        "Loading role metadata. role_dir=%s, path=%s",
        trace_id,
        role_dir,
        role_yaml_path,
    )

    # === PRE[0]: role yaml exists ===
    if not role_yaml_path.exists():
        logger.warning(
            "[PhaseConfigLoader][load_role_metadata][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Role metadata not found. role_dir=%s, path=%s",
            trace_id,
            role_dir,
            role_yaml_path,
        )
        raise FileNotFoundError(f"Role metadata not found: {role_yaml_path}")

    with role_yaml_path.open() as handle:
        raw = yaml.safe_load(handle)

    metadata = RoleMetadata(
        name=raw["name"],
        description=raw.get("description", ""),
        guides=list(raw.get("guides", [])),
        skills=list(raw.get("skills", [])),
        permissions=dict(raw.get("permissions", {})),
    )

    logger.info(
        "[PhaseConfigLoader][load_role_metadata][StepComplete] trace_id=%s | "
        "Loaded role metadata. role=%s, guides=%d, skills=%d",
        trace_id,
        metadata.name,
        len(metadata.guides),
        len(metadata.skills),
    )
    return metadata


# SEM_END orchestrator_v1.phase_config_loader.load_role_metadata:v1


def load_all_role_metadata() -> dict[str, RoleMetadata]:
    roles_root = get_docs_root() / "common" / "roles"
    metadata: dict[str, RoleMetadata] = {}
    for child in sorted(roles_root.iterdir()):
        role_yaml = child / "role.yaml"
        if child.is_dir() and role_yaml.exists():
            loaded = load_role_metadata(child.name)
            metadata[loaded.name] = loaded
    return metadata
