"""Parsers for V1 flow and phase runtime manifests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineStatus, SubRole
from workflow_runtime.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PhaseSpec:
    id: PhaseId
    description: str


@dataclass(frozen=True, slots=True)
class TransitionSpec:
    from_phase: PhaseId
    on_status: PipelineStatus
    to_phase: str
    reason: str


@dataclass(frozen=True, slots=True)
class FlowManifest:
    version: str
    start_phase: PhaseId
    end_phase: str
    phases: list[PhaseSpec]
    status_types: list[PipelineStatus]
    transitions: list[TransitionSpec]


@dataclass(frozen=True, slots=True)
class PromptSpec:
    sub_role: SubRole
    path: str


@dataclass(frozen=True, slots=True)
class PipelineStepConfig:
    role_dir: str
    prompt: PromptSpec
    model: str
    max_retries: int
    guardrails: list[str]


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    executor: PipelineStepConfig
    reviewer: PipelineStepConfig
    tester: PipelineStepConfig | None


@dataclass(frozen=True, slots=True)
class ExecuteStrategy:
    type: str
    max_concurrent: int


@dataclass(frozen=True, slots=True)
class PhaseRuntimeConfig:
    phase: str
    description: str
    role_dir: str | None
    pipeline: PipelineConfig | None
    default_worker_pipeline: PipelineConfig | None
    strategy: ExecuteStrategy | None


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    docs_root_alias: str
    docs_root_default: str
    prompts_root: str
    workspace_root_default: str
    tasks_root_default: str
    openhands: dict
    phases: dict[str, PhaseRuntimeConfig]


def _parse_prompt(raw: dict) -> PromptSpec:
    return PromptSpec(
        sub_role=SubRole(raw["sub_role"]),
        path=raw["path"],
    )


def _parse_step(raw: dict) -> PipelineStepConfig:
    return PipelineStepConfig(
        role_dir=raw["role_dir"],
        prompt=_parse_prompt(raw["prompt"]),
        model=raw["model"],
        max_retries=int(raw["max_retries"]),
        guardrails=list(raw.get("guardrails", [])),
    )


def _parse_pipeline(raw: dict | None) -> PipelineConfig | None:
    if raw is None:
        return None
    tester_raw = raw.get("tester")
    return PipelineConfig(
        executor=_parse_step(raw["executor"]),
        reviewer=_parse_step(raw["reviewer"]),
        tester=_parse_step(tester_raw) if tester_raw else None,
    )


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
        prompts_root=raw["runtime"]["prompts_root"],
        workspace_root_default=raw["runtime"]["workspace_root_default"],
        tasks_root_default=raw["runtime"]["tasks_root_default"],
        openhands=dict(raw["runtime"]["openhands"]),
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
