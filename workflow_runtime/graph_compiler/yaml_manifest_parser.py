"""Parsers for V1 flow and phase runtime manifests."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from workflow_runtime.graph_compiler.state_schema import (
    ExecutionBackend,
    PhaseId,
    PipelineStatus,
    SubRole,
)
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.phase_spec:v1
# type: CLASS
# use_case: Typed description of one top-level phase declared in flow.yaml.
# feature:
#   - Graph compilation should consume typed phase specs instead of raw YAML dicts
# pre:
#   -
# post:
#   -
# invariant:
#   - id matches one PhaseId used in the compiled graph
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PhaseId
# sft: define typed phase specification record loaded from flow manifest
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class PhaseSpec:
    id: PhaseId
    description: str


# SEM_END orchestrator_v1.yaml_manifest_parser.phase_spec:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.transition_spec:v1
# type: CLASS
# use_case: Typed transition rule between two top-level phases.
# feature:
#   - Phase routing is manifest-driven and depends on explicit status transitions
# pre:
#   -
# post:
#   -
# invariant:
#   - from_phase and on_status map to one flow.yaml transition row
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PhaseId
#   - PipelineStatus
# sft: define typed transition specification record for V1 phase routing
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class TransitionSpec:
    from_phase: PhaseId
    on_status: PipelineStatus
    to_phase: str
    reason: str


# SEM_END orchestrator_v1.yaml_manifest_parser.transition_spec:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.flow_manifest:v1
# type: CLASS
# use_case: Typed in-memory representation of flow.yaml.
# feature:
#   - Graph compilation uses one manifest object as the source of truth for phase order and transitions
# pre:
#   -
# post:
#   -
# invariant:
#   - phases and transitions remain aligned with one manifest version
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PhaseSpec
#   - TransitionSpec
# sft: define typed top-level flow manifest record for the V1 orchestrator graph
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class FlowManifest:
    version: str
    start_phase: PhaseId
    end_phase: str
    phases: list[PhaseSpec]
    status_types: list[PipelineStatus]
    transitions: list[TransitionSpec]


# SEM_END orchestrator_v1.yaml_manifest_parser.flow_manifest:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.prompt_spec:v1
# type: CLASS
# use_case: Typed prompt reference for one pipeline step.
# feature:
#   - Runtime config binds one sub-role to one markdown prompt path
# pre:
#   -
# post:
#   -
# invariant:
#   - sub_role matches the prompt contract and shared prompt fragments
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - SubRole
# sft: define typed prompt specification for one pipeline step
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class PromptSpec:
    sub_role: SubRole
    path: str


# SEM_END orchestrator_v1.yaml_manifest_parser.prompt_spec:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.step_execution_config:v1
# type: CLASS
# use_case: Typed execution backend selection for one executor/reviewer/tester step.
# feature:
#   - Runtime must choose one concrete backend and one optional strategy per step from YAML
# pre:
#   -
# post:
#   -
# invariant:
#   - backend stays within the supported execution backend enum
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - ExecutionBackend
# sft: define typed execution backend and strategy config for one task unit step
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class StepExecutionConfig:
    backend: ExecutionBackend
    strategy: str | None
    runtime_overrides: dict[str, Any] = field(default_factory=dict)


# SEM_END orchestrator_v1.yaml_manifest_parser.step_execution_config:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.pipeline_step_config:v1
# type: CLASS
# use_case: Typed configuration for one executor/reviewer/tester step.
# feature:
#   - TaskUnit runtime needs role prompt model retries guardrails and execution backend packaged per step
# pre:
#   -
# post:
#   -
# invariant:
#   - max_retries is an integer runtime bound for one step
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PromptSpec
#   - StepExecutionConfig
# sft: define typed pipeline step configuration for one task unit role including execution backend and strategy
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class PipelineStepConfig:
    role_dir: str
    prompt: PromptSpec
    execution: StepExecutionConfig
    model: str
    max_retries: int
    guardrails: list[str]


# SEM_END orchestrator_v1.yaml_manifest_parser.pipeline_step_config:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.pipeline_config:v1
# type: CLASS
# use_case: Groups executor reviewer and optional tester configs for one TaskUnit pipeline.
# feature:
#   - V1 phases and worker execution both reuse the same universal pipeline structure
# pre:
#   -
# post:
#   -
# invariant:
#   - executor and reviewer are always present
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PipelineStepConfig
# sft: define typed universal task unit pipeline config with executor reviewer and optional tester
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class PipelineConfig:
    executor: PipelineStepConfig
    reviewer: PipelineStepConfig | None
    tester: PipelineStepConfig | None


# SEM_END orchestrator_v1.yaml_manifest_parser.pipeline_config:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.execute_strategy:v1
# type: CLASS
# use_case: Typed execution-strategy settings for the execute phase.
# feature:
#   - V1 keeps execute planner-driven and bounded by max_concurrent
# pre:
#   -
# post:
#   -
# invariant:
#   - max_concurrent is a positive integer runtime bound
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define typed execute strategy config for planner-driven sequential runtime execution
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class ExecuteStrategy:
    type: str
    max_concurrent: int


# SEM_END orchestrator_v1.yaml_manifest_parser.execute_strategy:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.phase_runtime_config:v1
# type: CLASS
# use_case: Typed runtime configuration for one top-level phase.
# feature:
#   - Each phase binds a role pipeline and optional strategy while keeping one shared runtime schema
# pre:
#   -
# post:
#   -
# invariant:
#   - phase matches the key used in runtime_config.phases
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PipelineConfig
#   - ExecuteStrategy
# sft: define typed per-phase runtime configuration for the V1 orchestrator
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class PhaseRuntimeConfig:
    phase: str
    description: str
    role_dir: str | None
    pipeline: PipelineConfig | None
    default_worker_pipeline: PipelineConfig | None
    strategy: ExecuteStrategy | None


# SEM_END orchestrator_v1.yaml_manifest_parser.phase_runtime_config:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.task_repository_config:v1
# type: CLASS
# use_case: Typed configuration for one repository worktree provisioned inside a task workspace.
# feature:
#   - Runtime must provision task-local git worktrees from YAML instead of hardcoded single-repo paths
# pre:
#   -
# post:
#   -
# invariant:
#   - id stays stable across runtime state prompt context and filesystem layout
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define typed task repository config with source path sparse checkout and role defaults
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class TaskRepositoryConfig:
    id: str
    source_repo_root: str
    branch_prefix: str
    default_sparse_paths: list[str]
    default_for_roles: list[str]


# SEM_END orchestrator_v1.yaml_manifest_parser.task_repository_config:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.runtime_config:v1
# type: CLASS
# use_case: Typed in-memory representation of phases_and_roles.yaml.
# feature:
#   - Runtime compilation and prompt loading consume one typed config tree instead of raw YAML
# pre:
#   -
# post:
#   -
# invariant:
#   - phase names are the lookup keys used throughout the runtime
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PhaseRuntimeConfig
# sft: define typed runtime config record for docs paths openhands settings and per-phase pipelines
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    docs_root_alias: str
    docs_root_default: str
    methodology_root_default: str
    methodology_agents_entrypoint: str
    role_metadata_path: str
    force_injected_common_documents: list[str]
    prompts_root: str
    workspace_root_default: str
    tasks_root_default: str
    task_repositories: list[TaskRepositoryConfig]
    openhands: dict
    direct_llm: dict
    langchain_tools: dict
    phases: dict[str, PhaseRuntimeConfig]


# SEM_END orchestrator_v1.yaml_manifest_parser.runtime_config:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser._parse_prompt:v1
# type: METHOD
# use_case: Converts a raw prompt config dict into a typed PromptSpec.
# feature:
#   - Manifest parsing should normalize prompt dictionaries before higher-level runtime config assembly
# pre:
#   - raw contains sub_role and path keys
# post:
#   - returns a PromptSpec
# invariant:
#   - raw is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - KeyError: required keys are missing
# depends:
#   - PromptSpec
#   - SubRole
# sft: parse raw prompt configuration dict into typed prompt spec
# idempotent: true
# logs: -
def _parse_prompt(raw: dict) -> PromptSpec:
    return PromptSpec(
        sub_role=SubRole(raw["sub_role"]),
        path=raw["path"],
    )


# SEM_END orchestrator_v1.yaml_manifest_parser._parse_prompt:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser._parse_step:v1
# type: METHOD
# use_case: Converts a raw pipeline-step dict into a typed PipelineStepConfig.
# feature:
#   - Executor reviewer and tester steps must share one typed parser path
# pre:
#   - raw contains role_dir prompt model and max_retries keys
# post:
#   - returns a PipelineStepConfig
# invariant:
#   - raw is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - KeyError: required keys are missing
# depends:
#   - PipelineStepConfig
#   - _parse_prompt
# sft: parse raw step configuration dict into typed pipeline step config
# idempotent: true
# logs: -
def _parse_step(raw: dict) -> PipelineStepConfig:
    execution_raw = raw.get("execution", {})
    return PipelineStepConfig(
        role_dir=raw["role_dir"],
        prompt=_parse_prompt(raw["prompt"]),
        execution=StepExecutionConfig(
            backend=ExecutionBackend(str(execution_raw.get("backend", ExecutionBackend.OPENHANDS))),
            strategy=str(execution_raw["strategy"]) if execution_raw.get("strategy") is not None else None,
            runtime_overrides=dict(execution_raw.get("runtime_overrides", {})),
        ),
        model=raw["model"],
        max_retries=int(raw["max_retries"]),
        guardrails=list(raw.get("guardrails", [])),
    )


# SEM_END orchestrator_v1.yaml_manifest_parser._parse_step:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser._parse_pipeline:v1
# type: METHOD
# use_case: Converts a raw executor/reviewer/tester pipeline mapping into a typed PipelineConfig.
# feature:
#   - Phase-level and worker-level pipelines reuse the same parsing logic
# pre:
#   -
# post:
#   - returns PipelineConfig or None when the raw pipeline is absent
# invariant:
#   - raw is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - KeyError: executor or reviewer config is missing in a non-null pipeline
# depends:
#   - PipelineConfig
#   - _parse_step
# sft: parse raw pipeline mapping into typed executor reviewer and optional tester config
# idempotent: true
# logs: -
def _parse_pipeline(raw: dict | None) -> PipelineConfig | None:
    if raw is None:
        return None
    reviewer_raw = raw.get("reviewer")
    tester_raw = raw.get("tester")
    return PipelineConfig(
        executor=_parse_step(raw["executor"]),
        reviewer=_parse_step(reviewer_raw) if reviewer_raw else None,
        tester=_parse_step(tester_raw) if tester_raw else None,
    )


# SEM_END orchestrator_v1.yaml_manifest_parser._parse_pipeline:v1


def _parse_task_repositories(raw_items: list[dict] | None) -> list[TaskRepositoryConfig]:
    if not raw_items:
        return []
    repositories: list[TaskRepositoryConfig] = []
    for raw_item in raw_items:
        repositories.append(
            TaskRepositoryConfig(
                id=str(raw_item["id"]),
                source_repo_root=str(raw_item["source_repo_root"]),
                branch_prefix=str(raw_item.get("branch_prefix", "task")),
                default_sparse_paths=[str(path) for path in raw_item.get("default_sparse_paths", [])],
                default_for_roles=[str(role) for role in raw_item.get("default_for_roles", [])],
            )
        )
    return repositories


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.load_flow_manifest:v1
# type: METHOD
# use_case: Loads the top-level phase graph manifest from `orchestrator/config/flow.yaml`.
# feature:
#   - V1 graph consists only of collect/plan/execute/validate/human_gate phases
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0-D3
# pre:
#   - path exists
# post:
#   - returns typed FlowManifest
# invariant:
#   - input YAML is not mutated
# modifies (internal):
#   - file.orchestrator/config/flow.yaml
# emits (external):
#   -
# errors:
#   - FileNotFoundError: pre[0] violated
# depends:
#   - yaml.safe_load
# sft: load typed phase-flow manifest from YAML file for the V1 orchestrator
# idempotent: true
# logs: command: uv run pytest tests/ -v | path: orchestrator/config/flow.yaml
def load_flow_manifest(path: Path) -> FlowManifest:
    trace_id = ensure_trace_id()

    logger.info(
        "[YamlManifestParser][load_flow_manifest][ContextAnchor] trace_id=%s | "
        "Loading flow manifest. path=%s",
        trace_id,
        path,
    )

    # === PRE[0]: path exists ===
    if not path.exists():
        logger.warning(
            "[YamlManifestParser][load_flow_manifest][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Flow manifest not found. path=%s",
            trace_id,
            path,
        )
        raise FileNotFoundError(f"Flow manifest not found: {path}")

    with path.open() as handle:
        raw = yaml.safe_load(handle)

    manifest = FlowManifest(
        version=raw["version"],
        start_phase=PhaseId(raw["start_phase"]),
        end_phase=raw["end_phase"],
        phases=[
            PhaseSpec(id=PhaseId(phase["id"]), description=phase.get("description", ""))
            for phase in raw["phases"]
        ],
        status_types=[PipelineStatus(status) for status in raw["status_types"]],
        transitions=[
            TransitionSpec(
                from_phase=PhaseId(transition["from"]),
                on_status=PipelineStatus(transition["on_status"]),
                to_phase=transition["to"],
                reason=transition["reason"],
            )
            for transition in raw["transitions"]
        ],
    )

    logger.info(
        "[YamlManifestParser][load_flow_manifest][StepComplete] trace_id=%s | "
        "Loaded flow manifest. phases=%d, transitions=%d",
        trace_id,
        len(manifest.phases),
        len(manifest.transitions),
    )
    return manifest


# SEM_END orchestrator_v1.yaml_manifest_parser.load_flow_manifest:v1


# SEM_BEGIN orchestrator_v1.yaml_manifest_parser.load_runtime_config:v1
# type: METHOD
# use_case: Loads the runtime phase config from `orchestrator/config/phases_and_roles.yaml`.
# feature:
#   - runtime config describes universal TaskUnit and OpenHands integration path
#   - Task card 2026-03-24_1800__multi-agent-system-design, D0-D7
# pre:
#   - path exists
# post:
#   - returns RuntimeConfig with per-phase pipeline settings
# invariant:
#   - input YAML is not mutated
# modifies (internal):
#   - file.orchestrator/config/phases_and_roles.yaml
# emits (external):
#   -
# errors:
#   - FileNotFoundError: pre[0] violated
# depends:
#   - yaml.safe_load
# sft: load runtime phase and role configuration for the V1 orchestrator
# idempotent: true
# logs: command: uv run pytest tests/ -v | path: orchestrator/config/phases_and_roles.yaml
def load_runtime_config(path: Path) -> RuntimeConfig:
    trace_id = ensure_trace_id()

    logger.info(
        "[YamlManifestParser][load_runtime_config][ContextAnchor] trace_id=%s | "
        "Loading runtime config. path=%s",
        trace_id,
        path,
    )

    # === PRE[0]: path exists ===
    if not path.exists():
        logger.warning(
            "[YamlManifestParser][load_runtime_config][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
            "Runtime config not found. path=%s",
            trace_id,
            path,
        )
        raise FileNotFoundError(f"Runtime config not found: {path}")

    with path.open() as handle:
        raw = yaml.safe_load(handle)

    phases: dict[str, PhaseRuntimeConfig] = {}
    for phase_name, phase_raw in raw["phases"].items():
        phases[phase_name] = PhaseRuntimeConfig(
            phase=phase_raw["phase"],
            description=phase_raw.get("description", ""),
            role_dir=phase_raw.get("role_dir"),
            pipeline=_parse_pipeline(phase_raw.get("pipeline")),
            default_worker_pipeline=_parse_pipeline(phase_raw.get("default_worker_pipeline")),
            strategy=(
                ExecuteStrategy(
                    type=phase_raw["strategy"]["type"],
                    max_concurrent=int(phase_raw["strategy"]["max_concurrent"]),
                )
                if phase_raw.get("strategy")
                else None
            ),
        )

    config = RuntimeConfig(
        docs_root_alias=raw["runtime"]["docs_root_alias"],
        docs_root_default=raw["runtime"]["docs_root_default"],
        methodology_root_default=raw["runtime"].get(
            "methodology_root_default",
            raw["runtime"]["docs_root_default"],
        ),
        methodology_agents_entrypoint=raw["runtime"].get(
            "methodology_agents_entrypoint",
            "AGENTS.md",
        ),
        role_metadata_path=raw["runtime"].get(
            "role_metadata_path",
            "Technical Docs/common/roles/{role_dir}/role.yaml",
        ),
        force_injected_common_documents=list(
            raw["runtime"].get("force_injected_common_documents", [])
        ),
        prompts_root=raw["runtime"]["prompts_root"],
        workspace_root_default=raw["runtime"]["workspace_root_default"],
        tasks_root_default=raw["runtime"]["tasks_root_default"],
        task_repositories=_parse_task_repositories(raw["runtime"].get("task_repositories")),
        openhands=dict(raw["runtime"]["openhands"]),
        direct_llm=dict(raw["runtime"].get("direct_llm", {})),
        langchain_tools=dict(raw["runtime"].get("langchain_tools", {})),
        phases=phases,
    )

    logger.info(
        "[YamlManifestParser][load_runtime_config][StepComplete] trace_id=%s | "
        "Loaded runtime config. phases=%d",
        trace_id,
        len(config.phases),
    )
    return config


# SEM_END orchestrator_v1.yaml_manifest_parser.load_runtime_config:v1
