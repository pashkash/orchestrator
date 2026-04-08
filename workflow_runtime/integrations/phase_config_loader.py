"""Cached loaders for V1 orchestrator manifests and role metadata."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
import re

import yaml

from workflow_runtime.graph_compiler.yaml_manifest_parser import (
    FlowManifest,
    RuntimeConfig,
    TaskRepositoryConfig,
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
WORKSPACE_CONFIG_PATH = Path("/root/squadder.code-workspace")


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
    force_injected_documents: list[str] = field(default_factory=list)
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
    workspace_path = get_workspace_folder_path(runtime.docs_root_alias)
    if workspace_path is not None:
        return workspace_path
    return Path(runtime.docs_root_default)


# SEM_END orchestrator_v1.phase_config_loader.get_docs_root:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_task_repositories:v1
# type: METHOD
# use_case: Возвращает configured task repositories из cached runtime config.
# feature:
#   - Multi-repo workspace provisioning опирается на runtime.task_repositories как source of truth
# pre:
#   - runtime config loaded successfully
# post:
#   - returns a copy-like list of TaskRepositoryConfig values
# invariant:
#   - cached runtime config object is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_runtime_config
# sft: return the configured task repositories from runtime config for multi-repo workspace setup
# idempotent: true
# logs: -
def get_task_repositories() -> list[TaskRepositoryConfig]:
    return list(get_runtime_config().task_repositories)
# SEM_END orchestrator_v1.phase_config_loader.get_task_repositories:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_primary_task_repository:v1
# type: METHOD
# use_case: Возвращает primary repository по порядку runtime.task_repositories.
# feature:
#   - Некоторые bootstrap/debug path-ы используют "первый configured repo" как default workspace when no override is provided
# pre:
#   - runtime config is readable
# post:
#   - returns the first configured task repository or None
# invariant:
#   - configured repository order remains the source of truth
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_task_repositories
# sft: return the first configured task repository as the default primary workspace repo
# idempotent: true
# logs: -
def get_primary_task_repository() -> TaskRepositoryConfig | None:
    task_repositories = get_task_repositories()
    return task_repositories[0] if task_repositories else None
# SEM_END orchestrator_v1.phase_config_loader.get_primary_task_repository:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.build_role_workspace_repo_map:v1
# type: METHOD
# use_case: Строит mapping role_dir -> repo_id для выбора рабочего репозитория по умолчанию.
# feature:
#   - Planner/executor path должен уметь выбрать repo по domain role без хардкода в Python
# pre:
#   - repository ids and default_for_roles are defined in runtime.task_repositories
# post:
#   - returns first-match mapping for every declared role
# invariant:
#   - once a role is mapped, later repositories do not override it
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_task_repositories
# sft: build the default role to repository mapping from runtime task repository config
# idempotent: true
# logs: -
def build_role_workspace_repo_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for repository in get_task_repositories():
        for role in repository.default_for_roles:
            normalized_role = str(role).strip()
            if normalized_role and normalized_role not in mapping:
                mapping[normalized_role] = repository.id
    return mapping
# SEM_END orchestrator_v1.phase_config_loader.build_role_workspace_repo_map:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.resolve_role_working_directory:v1
# type: METHOD
# use_case: Разрешает рабочую директорию role step-а внутри task-local multi-repo workspace.
# feature:
#   - Role-specific steps должны попадать в свой repo worktree, но иметь fallback на общий task_worktree_root
# pre:
#   - task_worktree_root is a runtime-visible workspace path
# post:
#   - returns repo-specific worktree when role mapping exists, otherwise task_worktree_root
# invariant:
#   - role_dir string is only used for lookup, not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - build_role_workspace_repo_map
# sft: resolve the working directory for a runtime role step inside the multi-repo task workspace
# idempotent: true
# logs: -
def resolve_role_working_directory(
    *,
    role_dir: str | None,
    task_worktree_root: str,
    task_workspace_repos: dict[str, str] | None = None,
    role_workspace_repo_map: dict[str, str] | None = None,
) -> str:
    normalized_role_dir = str(role_dir or "").strip()
    repo_id = str((role_workspace_repo_map or {}).get(normalized_role_dir) or "").strip()
    if repo_id:
        resolved = str((task_workspace_repos or {}).get(repo_id) or "").strip()
        if resolved:
            return resolved
    return str(task_worktree_root)
# SEM_END orchestrator_v1.phase_config_loader.resolve_role_working_directory:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_methodology_root_host:v1
# type: METHOD
# use_case: Возвращает host-visible methodology/docs root для bootstrap шагов.
# feature:
#   - runtime должен различать host docs root и runtime-visible docs root inside task execution environment
# pre:
#   - runtime config is readable
# post:
#   - returns workspace alias path when available, otherwise configured default
# invariant:
#   - host root resolution does not depend on task-local workspace state
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_workspace_folder_path
# sft: resolve the host-visible methodology root path from runtime config and workspace aliases
# idempotent: true
# logs: -
def get_methodology_root_host() -> Path:
    runtime = get_runtime_config()
    workspace_path = get_workspace_folder_path(runtime.docs_root_alias)
    if workspace_path is not None:
        return workspace_path
    return Path(runtime.methodology_root_default)
# SEM_END orchestrator_v1.phase_config_loader.get_methodology_root_host:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_methodology_root_runtime:v1
# type: METHOD
# use_case: Возвращает runtime-visible methodology root, который увидят драйверы и tool-capable агенты.
# feature:
#   - runtime-visible root может отличаться от host root через env/config override
# pre:
#   - runtime config is readable
# post:
#   - returns env override, configured runtime root, or host root fallback
# invariant:
#   - fallback order is env -> runtime config -> host root
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_methodology_root_host
# sft: resolve the runtime-visible methodology root with env and config overrides
# idempotent: true
# logs: -
def get_methodology_root_runtime() -> Path:
    runtime = get_runtime_config()
    env_override = os.getenv("WORKFLOW_METHODOLOGY_ROOT_RUNTIME")
    if env_override:
        return Path(env_override)
    configured = runtime.openhands.get("methodology_root_runtime")
    if configured:
        return Path(str(configured))
    return get_methodology_root_host()
# SEM_END orchestrator_v1.phase_config_loader.get_methodology_root_runtime:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.resolve_methodology_entrypoint:v1
# type: METHOD
# use_case: Находит AGENTS entrypoint в host-visible или runtime-visible methodology tree.
# feature:
#   - Prompt composition и task bootstrap должны иметь стабильный путь до AGENTS.md даже при rebased docs roots
# pre:
#   - methodology root is resolvable
# post:
#   - returns the first existing entrypoint candidate or the preferred default path
# invariant:
#   - candidate search order is stable
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_methodology_root_runtime
#   - get_methodology_root_host
# sft: resolve the AGENTS methodology entrypoint for host-visible or runtime-visible docs roots
# idempotent: true
# logs: -
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
# SEM_END orchestrator_v1.phase_config_loader.resolve_methodology_entrypoint:v1


def _normalize_workspace_folder_name(name: str) -> str:
    trimmed = " ".join(str(name).strip().split())
    trimmed = re.sub(r"^[^\w]+", "", trimmed).strip()
    return trimmed


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_workspace_folder_map:v1
# type: METHOD
# use_case: Читает workspace file и строит alias -> absolute path map для runtime path resolution.
# feature:
#   - Runtime aliases вроде `Technical Docs` и repo workspace names должны резолвиться из workspace config, а не только из hardcoded defaults
# pre:
#   - workspace config file may or may not exist
# post:
#   - returns alias map from configured workspace folders
# invariant:
#   - alias normalization is deterministic for one workspace config snapshot
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - WORKSPACE_CONFIG_PATH
# sft: build a cached alias map from the workspace configuration file for runtime path resolution
# idempotent: true
# logs: -
@lru_cache(maxsize=1)
def get_workspace_folder_map() -> dict[str, Path]:
    if not WORKSPACE_CONFIG_PATH.exists():
        return {}
    raw = json.loads(WORKSPACE_CONFIG_PATH.read_text())
    mapping: dict[str, Path] = {}
    for folder in raw.get("folders", []):
        folder_name = str(folder.get("name", "")).strip()
        folder_path = str(folder.get("path", "")).strip()
        if not folder_name or not folder_path:
            continue
        resolved_path = Path(folder_path).resolve()
        aliases = {
            folder_name,
            _normalize_workspace_folder_name(folder_name),
        }
        for alias in aliases:
            normalized_alias = " ".join(alias.split()).strip()
            if normalized_alias:
                mapping[normalized_alias] = resolved_path
    return mapping
# SEM_END orchestrator_v1.phase_config_loader.get_workspace_folder_map:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_workspace_folder_path:v1
# type: METHOD
# use_case: Возвращает absolute workspace path по alias name из cached workspace folder map.
# feature:
#   - Runtime path resolution должен уметь искать aliases вроде `Technical Docs` или repo names из workspace config
# pre:
#   - alias is string-like
# post:
#   - returns matching Path or None
# invariant:
#   - alias normalization is whitespace-insensitive
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_workspace_folder_map
# sft: resolve a workspace folder alias into an absolute path using the cached workspace folder map
# idempotent: true
# logs: -
def get_workspace_folder_path(alias: str) -> Path | None:
    normalized_alias = " ".join(str(alias).split()).strip()
    if not normalized_alias:
        return None
    return get_workspace_folder_map().get(normalized_alias)
# SEM_END orchestrator_v1.phase_config_loader.get_workspace_folder_path:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.get_runtime_alias_map:v1
# type: METHOD
# use_case: Собирает canonical runtime alias map для docs root и project-specific aliases.
# feature:
#   - Runtime path resolution должен одинаково понимать workspace aliases и derived aliases вроде `Project Guides`/`Project Skills`
# pre:
#   - runtime config is readable
# post:
#   - returns alias map with docs-root-derived entries
# invariant:
#   - canonical docs root alias is always present in the map
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_workspace_folder_map
# sft: build the canonical runtime alias map including docs-root-derived project aliases
# idempotent: true
# logs: -
@lru_cache(maxsize=1)
def get_runtime_alias_map() -> dict[str, Path]:
    runtime = get_runtime_config()
    alias_map = dict(get_workspace_folder_map())
    docs_root = alias_map.get(runtime.docs_root_alias, Path(runtime.docs_root_default))
    alias_map.setdefault(runtime.docs_root_alias, Path(docs_root))
    alias_map.setdefault("Project Guides", Path(docs_root) / "project_specific" / "guides")
    alias_map.setdefault("Project Skills", Path(docs_root) / "project_specific" / "skills")
    return alias_map
# SEM_END orchestrator_v1.phase_config_loader.get_runtime_alias_map:v1


def _resolve_runtime_alias_path_with_map(raw_path: str, alias_map: dict[str, Path]) -> Path | None:
    normalized = " ".join(str(raw_path).strip().split())
    if not normalized:
        return None
    for alias, base_path in sorted(alias_map.items(), key=lambda item: len(item[0]), reverse=True):
        alias_prefix = f"{alias}/"
        if normalized == alias:
            return Path(base_path)
        if normalized.startswith(alias_prefix):
            return Path(base_path) / normalized[len(alias_prefix) :]
    return None


def _normalize_absolute_runtime_alias_string_with_map(raw_path: str, alias_map: dict[str, Path]) -> str | None:
    normalized = str(raw_path).strip()
    if not normalized.startswith("/"):
        return None
    for alias, base_path in alias_map.items():
        duplicated_prefix = str(Path(base_path) / alias)
        if normalized == duplicated_prefix:
            return str(base_path)
        duplicated_prefix_with_sep = duplicated_prefix + "/"
        if normalized.startswith(duplicated_prefix_with_sep):
            return str(Path(base_path) / normalized[len(duplicated_prefix_with_sep) :])
    return None


# SEM_BEGIN orchestrator_v1.phase_config_loader.build_runtime_alias_map_for_docs_root:v1
# type: METHOD
# use_case: Ребейзит canonical runtime alias map на другой docs root, например task-local projected docs.
# feature:
#   - Tool/runtime code должен поддерживать и host docs root, и task-local runtime-visible docs root с теми же alias names
# pre:
#   - docs_root is a path-like docs root candidate
# post:
#   - returns alias map rebased onto the provided docs root where possible
# invariant:
#   - aliases outside canonical docs root keep their original absolute paths
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - get_runtime_alias_map
# sft: rebase the canonical runtime alias map onto an alternate docs root such as a task-local projected docs tree
# idempotent: true
# logs: -
def build_runtime_alias_map_for_docs_root(docs_root: Path | str) -> dict[str, Path]:
    canonical_docs_root = get_docs_root().resolve(strict=False)
    rebased_docs_root = Path(docs_root)
    alias_map: dict[str, Path] = {}
    for alias, base_path in get_runtime_alias_map().items():
        base_path_obj = Path(base_path)
        try:
            suffix = base_path_obj.resolve(strict=False).relative_to(canonical_docs_root)
            alias_map[alias] = rebased_docs_root / suffix
        except ValueError:
            alias_map[alias] = base_path_obj
    return alias_map
# SEM_END orchestrator_v1.phase_config_loader.build_runtime_alias_map_for_docs_root:v1


def resolve_runtime_alias_path(raw_path: str) -> Path | None:
    return _resolve_runtime_alias_path_with_map(raw_path, get_runtime_alias_map())


# SEM_BEGIN orchestrator_v1.phase_config_loader.normalize_runtime_alias_string:v1
# type: METHOD
# use_case: Нормализует один runtime alias string по canonical alias map.
# feature:
#   - Drivers и prompt helpers получают path strings из config/docs и должны переводить их в реальные absolute paths
# pre:
#   - raw_path is string-like
# post:
#   - returns normalized absolute or alias-resolved path string when possible
# invariant:
#   - original string is returned unchanged when no alias rule matched
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - resolve_runtime_alias_path
# sft: normalize a runtime alias string against the canonical runtime alias map
# idempotent: true
# logs: -
def normalize_runtime_alias_string(raw_path: str) -> str:
    normalized = str(raw_path).strip()
    alias_resolved = resolve_runtime_alias_path(normalized)
    if alias_resolved is not None:
        return str(alias_resolved)
    absolute_normalized = _normalize_absolute_runtime_alias_string_with_map(
        normalized, get_runtime_alias_map()
    )
    if absolute_normalized is not None:
        return absolute_normalized
    return normalized
# SEM_END orchestrator_v1.phase_config_loader.normalize_runtime_alias_string:v1


# SEM_BEGIN orchestrator_v1.phase_config_loader.normalize_runtime_alias_string_for_docs_roots:v1
# type: METHOD
# use_case: Нормализует runtime alias path с учётом одного или нескольких rebased docs roots.
# feature:
#   - Tool/runtime code должен понимать и canonical docs alias map, и task-local rebased docs roots
# pre:
#   - docs_roots may contain host-visible and runtime-visible docs roots
# post:
#   - returns the first matching normalized path string or the original string when nothing matches
# invariant:
#   - alias maps are probed in deterministic order
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - -
# depends:
#   - build_runtime_alias_map_for_docs_root
# sft: normalize runtime alias strings against the canonical and rebased docs root alias maps
# idempotent: true
# logs: -
def normalize_runtime_alias_string_for_docs_roots(
    raw_path: str,
    *,
    docs_roots: list[Path | str],
) -> str:
    normalized = str(raw_path).strip()
    alias_maps: list[dict[str, Path]] = [get_runtime_alias_map()]
    seen_roots: set[str] = set()
    for docs_root in docs_roots:
        for candidate in (Path(docs_root), Path(docs_root).resolve(strict=False)):
            key = str(candidate)
            if key in seen_roots:
                continue
            seen_roots.add(key)
            alias_maps.append(build_runtime_alias_map_for_docs_root(candidate))
    for alias_map in alias_maps:
        alias_resolved = _resolve_runtime_alias_path_with_map(normalized, alias_map)
        if alias_resolved is not None:
            return str(alias_resolved)
    for alias_map in alias_maps:
        absolute_normalized = _normalize_absolute_runtime_alias_string_with_map(normalized, alias_map)
        if absolute_normalized is not None:
            return absolute_normalized
    return normalized
# SEM_END orchestrator_v1.phase_config_loader.normalize_runtime_alias_string_for_docs_roots:v1


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
    resolved = raw_path.replace("{role_dir}", role_dir or "")
    alias_resolved = resolve_runtime_alias_path(resolved)
    if alias_resolved is not None:
        return alias_resolved
    return Path(normalize_runtime_alias_string(resolved))


# SEM_END orchestrator_v1.phase_config_loader.resolve_runtime_path:v1


def get_role_metadata_path(role_dir: str) -> Path:
    runtime = get_runtime_config()
    return resolve_runtime_path(runtime.role_metadata_path, role_dir)


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
@lru_cache(maxsize=None)
def load_role_metadata(role_dir: str) -> RoleMetadata:
    trace_id = ensure_trace_id()
    role_yaml_path = get_role_metadata_path(role_dir)

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
        force_injected_documents=list(raw.get("force_injected_documents", [])),
        permissions=dict(raw.get("permissions", {})),
    )

    logger.info(
        "[PhaseConfigLoader][load_role_metadata][StepComplete] trace_id=%s | "
        "Loaded role metadata. role=%s, force_injected_documents=%d",
        trace_id,
        metadata.name,
        len(metadata.force_injected_documents),
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
