"""Cached loaders for V1 orchestrator manifests and role metadata."""

from __future__ import annotations

import os
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
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)

ORCHESTRATOR_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = ORCHESTRATOR_ROOT / "config"
FLOW_MANIFEST_PATH = CONFIG_ROOT / "flow.yaml"
PHASES_RUNTIME_PATH = CONFIG_ROOT / "phases_and_roles.yaml"


# SEM_BEGIN orchestrator_v1.phase_config_loader.role_metadata:v1
# type: CLASS
# use_case: Typed projection of one role.yaml entry used by prompt composition and permission lookup.
# feature:
#   - Runtime reads role metadata from docs/common/roles without coupling to raw YAML dictionaries
#   - Task card 2026-03-24_1800__multi-agent-system-design, D1
# pre:
#   -
# post:
#   -
# invariant:
#   - name remains the stable runtime identifier for one role definition
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define typed role metadata record loaded from docs role yaml
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class RoleMetadata:
    name: str
    description: str
    guides: list[str] = field(default_factory=list)
    skills: list[str] = field(default_factory=list)
    permissions: dict = field(default_factory=dict)


# SEM_END orchestrator_v1.phase_config_loader.role_metadata:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_flow_manifest:v1
# type: METHOD
# use_case: Returns the cached typed flow manifest from the configured flow.yaml path.
# feature:
#   - Graph compilation repeatedly reuses one parsed flow manifest instead of reparsing YAML on every call
# pre:
#   -
# post:
#   - returns a cached FlowManifest
# invariant:
#   - FLOW_MANIFEST_PATH stays the source path for this loader
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - FileNotFoundError: flow manifest path does not exist
# depends:
#   - load_flow_manifest
# sft: return cached typed flow manifest loaded from the orchestrator flow yaml path
# idempotent: true
# logs: -
@lru_cache(maxsize=1)
def get_flow_manifest() -> FlowManifest:
    return load_flow_manifest(FLOW_MANIFEST_PATH)


# SEM_END orchestrator_v1.phase_config_loader.get_flow_manifest:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_runtime_config:v1
# type: METHOD
# use_case: Returns the cached typed runtime config from phases_and_roles.yaml.
# feature:
#   - Runtime components share one parsed config tree instead of reparsing YAML on every call
# pre:
#   -
# post:
#   - returns a cached RuntimeConfig
# invariant:
#   - PHASES_RUNTIME_PATH stays the source path for this loader
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - FileNotFoundError: runtime config path does not exist
# depends:
#   - load_runtime_config
# sft: return cached typed runtime config loaded from the phases and roles yaml path
# idempotent: true
# logs: -
@lru_cache(maxsize=1)
def get_runtime_config() -> RuntimeConfig:
    return load_runtime_config(PHASES_RUNTIME_PATH)


# SEM_END orchestrator_v1.phase_config_loader.get_runtime_config:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_docs_root:v1
# type: METHOD
# use_case: Resolves the default docs root path from cached runtime config.
# feature:
#   - Prompt and role metadata loading derive their docs root from one runtime config field
# pre:
#   -
# post:
#   - returns the default docs root path
# invariant:
#   - runtime config remains the source of truth for docs root resolution
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - get_runtime_config
# sft: resolve the default docs root path from cached orchestrator runtime config
# idempotent: true
# logs: -
def get_docs_root() -> Path:
    runtime = get_runtime_config()
    return Path(runtime.docs_root_default)


# SEM_END orchestrator_v1.phase_config_loader.get_docs_root:v1


def get_methodology_root_host() -> Path:
    runtime = get_runtime_config()
    return Path(runtime.methodology_root_default)


def get_methodology_root_runtime() -> Path:
    runtime = get_runtime_config()
    env_override = os.getenv("WORKFLOW_METHODOLOGY_ROOT_RUNTIME")
    if env_override:
        return Path(env_override)
    configured = runtime.openhands.get("methodology_root_runtime")
    if configured:
        return Path(str(configured))
    return get_methodology_root_host()


def resolve_methodology_entrypoint(*, runtime_visible: bool = True) -> Path:
    runtime = get_runtime_config()
    root = get_methodology_root_runtime() if runtime_visible else get_methodology_root_host()
    candidates = [
        root / runtime.methodology_agents_entrypoint,
        root / "docs" / runtime.methodology_agents_entrypoint,
    ]
    if runtime_visible and root != get_methodology_root_host():
        return candidates[0]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


# SEM_BEGIN orchestrator_v1.phase_config_loader.resolve_runtime_path:v1
# type: METHOD
# use_case: Resolves a runtime path alias into an absolute filesystem path.
# feature:
#   - Runtime config can reference docs-root aliases while code still opens concrete files
#   - Task card 2026-03-24_1800__multi-agent-system-design, D1
# pre:
#   - raw_path is not empty
# post:
#   - returns an absolute or directly usable Path for runtime consumers
# invariant:
#   - raw_path string is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - get_runtime_config
#   - get_docs_root
# sft: resolve runtime path aliases into concrete filesystem paths for prompt and docs loading
# idempotent: true
# logs: -
def resolve_runtime_path(raw_path: str, role_dir: str | None = None) -> Path:
    runtime = get_runtime_config()
    resolved = raw_path.replace("{role_dir}", role_dir or "")
    if resolved.startswith(f"{runtime.docs_root_alias}/"):
        suffix = resolved[len(runtime.docs_root_alias) + 1:]
        return get_docs_root() / suffix
    return Path(resolved)


# SEM_END orchestrator_v1.phase_config_loader.resolve_runtime_path:v1


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


# SEM_BEGIN orchestrator_v1.phase_config_loader.load_all_role_metadata:v1
# type: METHOD
# use_case: Loads all available role metadata records from the shared docs roles directory.
# feature:
#   - Supervisor and tooling can inspect all known roles from the same docs-backed source of truth
#   - Task card 2026-03-24_1800__multi-agent-system-design, D1
# pre:
#   - docs/common/roles directory exists
# post:
#   - returns a mapping keyed by role metadata name
# invariant:
#   - only role directories with role.yaml are loaded
# modifies (internal):
#   - file.docs/common/roles
# emits (external):
#   -
# errors:
#   - FileNotFoundError: roles root does not exist
# depends:
#   - load_role_metadata
# sft: load all docs-backed role metadata entries for orchestrator runtime introspection
# idempotent: true
# logs: -
def load_all_role_metadata() -> dict[str, RoleMetadata]:
    trace_id = ensure_trace_id()
    roles_root = get_docs_root() / "common" / "roles"
    logger.info(
        "[PhaseConfigLoader][load_all_role_metadata][ContextAnchor] trace_id=%s | "
        "Loading all role metadata. roles_root=%s",
        trace_id,
        roles_root,
    )
    logger.info(
        "[PhaseConfigLoader][load_all_role_metadata][PreCheck] trace_id=%s | "
        "Checking roles root exists. path=%s",
        trace_id,
        roles_root,
    )
    if not roles_root.exists():
        logger.warning(
            "[PhaseConfigLoader][load_all_role_metadata][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Roles root not found. path=%s",
            trace_id,
            roles_root,
        )
        raise FileNotFoundError(f"Roles root not found: {roles_root}")
    metadata: dict[str, RoleMetadata] = {}
    for child in sorted(roles_root.iterdir()):
        role_yaml = child / "role.yaml"
        if child.is_dir() and role_yaml.exists():
            loaded = load_role_metadata(child.name)
            metadata[loaded.name] = loaded
    logger.info(
        "[PhaseConfigLoader][load_all_role_metadata][StepComplete] trace_id=%s | "
        "Loaded all role metadata. roles=%d",
        trace_id,
        len(metadata),
    )
    return metadata


# SEM_END orchestrator_v1.phase_config_loader.load_all_role_metadata:v1
