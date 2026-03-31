"""OpenHands-backed runtime driver."""

from __future__ import annotations

import re
from typing import Any

import yaml

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.graph_compiler.state_schema import PipelineStatus, SubRole
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.openhands_http_api import OpenHandsHttpApi
from workflow_runtime.integrations.openhands_runtime import (
    OPENHANDS_EVENT_SEARCH_LIMIT_MAX,
)
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)

_YAML_BLOCK_RE = re.compile(r"```(?:yaml|yml)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


# SEM_BEGIN orchestrator_v1.openhands_driver._extract_texts:v1
# type: METHOD
# use_case: Recursively extracts all text leaves from nested OpenHands state/event payloads.
# feature:
#   - Driver parsing must normalize heterogenous OpenHands response shapes before YAML extraction
# pre:
#   -
# post:
#   - returns a flat list of text fragments discovered in the nested payload
# invariant:
#   - input node is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: recursively collect text fragments from nested OpenHands state and event payloads
# idempotent: true
# logs: -
def _extract_texts(node: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "text" and isinstance(value, str):
                texts.append(value)
            else:
                texts.extend(_extract_texts(value))
    elif isinstance(node, list):
        for item in node:
            texts.extend(_extract_texts(item))
    return texts


# SEM_END orchestrator_v1.openhands_driver._extract_texts:v1


# SEM_BEGIN orchestrator_v1.openhands_driver._coerce_payload:v1
# type: METHOD
# use_case: Extracts the first valid YAML object from the OpenHands agent text response.
# feature:
#   - OpenHands agent may return text interleaved with YAML blocks
# pre:
#   - raw_text is not empty
# post:
#   - returns a parsed dict from the last YAML block
# invariant:
#   - raw_text is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   - ValueError: no YAML block found
# depends:
#   - yaml.safe_load
# sft: extract the last valid YAML mapping from OpenHands agent raw text response
# idempotent: true
# logs: -
def _coerce_payload(raw_text: str) -> dict[str, Any]:
    blocks = _YAML_BLOCK_RE.findall(raw_text)
    candidates = list(reversed(blocks)) if blocks else [raw_text]
    for candidate in candidates:
        try:
            loaded = yaml.safe_load(candidate)
        except yaml.YAMLError:
            continue
        if isinstance(loaded, dict):
            return loaded
    raise ValueError("No YAML object could be parsed from OpenHands output")


# SEM_END orchestrator_v1.openhands_driver._coerce_payload:v1


# SEM_BEGIN orchestrator_v1.openhands_driver._status_for_parse_failure:v1
# type: METHOD
# use_case: Maps parse failures to the phase-specific fix status expected by TaskUnit.
# feature:
#   - Reviewer and tester parse failures must not collapse into executor repair statuses
# pre:
#   - sub_role is one of executor/reviewer/tester
# post:
#   - returns the correct NEEDS_FIX_* PipelineStatus for that sub-role
# invariant:
#   - no runtime state is mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - PipelineStatus
#   - SubRole
# sft: map one task unit sub-role to the corresponding parse-failure pipeline status
# idempotent: true
# logs: -
def _status_for_parse_failure(sub_role: SubRole) -> PipelineStatus:
    if sub_role == SubRole.REVIEWER:
        return PipelineStatus.NEEDS_FIX_REVIEW
    if sub_role == SubRole.TESTER:
        return PipelineStatus.NEEDS_FIX_TESTS
    return PipelineStatus.NEEDS_FIX_EXECUTOR


# SEM_END orchestrator_v1.openhands_driver._status_for_parse_failure:v1


# SEM_BEGIN orchestrator_v1.openhands_driver.openhands_driver:v1
# type: CLASS
# use_case: Real runtime driver that executes TaskUnit steps through OpenHands.
# feature:
#   - Phase 3 keeps the same TaskUnit contract while replacing the backend with OpenHands REST execution
#   - Task card 2026-03-24_1800__multi-agent-system-design, D4-D5
# pre:
#   -
# post:
#   -
# invariant:
#   - the driver returns normalized DriverResult objects regardless of raw OpenHands response shape
# modifies (internal):
#   -
# emits (external):
#   - external.openhands_server
# errors:
#   - RuntimeError: OpenHands execution failed
# depends:
#   - OpenHandsHttpApi
# sft: implement real OpenHands-backed runtime driver for one universal task unit step
# idempotent: false
# logs: query: OpenHandsDriver trace_id
class OpenHandsDriver(BaseDriver):
    # SEM_BEGIN orchestrator_v1.openhands_driver.openhands_driver.__init__:v1
    # type: METHOD
    # use_case: Stores the OpenHands API client and runtime LLM/tool settings for future task execution.
    # feature:
    #   - Runtime graph compilation injects one configured OpenHands backend into the universal TaskUnit
    # pre:
    #   - api is a ready OpenHandsHttpApi client
    # post:
    #   - driver keeps the provided API client credentials base URL mode and tools
    # invariant:
    #   - provided configuration is reused for all subsequent run_task calls
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   -
    # depends:
    #   - OpenHandsHttpApi
    # sft: initialize OpenHands runtime driver with API client llm settings cli mode and tool list
    # idempotent: false
    # logs: -
    def __init__(
        self,
        *,
        api: OpenHandsHttpApi,
        llm_api_key: str,
        llm_base_url: str,
        cli_mode: bool,
        tools: list[str],
    ) -> None:
        self._api = api
        self._llm_api_key = llm_api_key
        self._llm_base_url = llm_base_url
        self._cli_mode = cli_mode
        self._tools = tools

    # SEM_END orchestrator_v1.openhands_driver.openhands_driver.__init__:v1

    # SEM_BEGIN orchestrator_v1.openhands_driver.openhands_driver.run_task:v1
    # type: METHOD
    # use_case: Runs a single TaskUnit step through the real OpenHands agent server.
    # feature:
    #   - Phase 3 uses the same TaskUnit contract but the execution backend is real
    #   - orchestrator/config/phases_and_roles.yaml -> runtime.openhands + per-step model
    # pre:
    #   - request.prompt is not empty
    #   - llm_api_key is configured
    # post:
    #   - returns a DriverResult with status, payload, and conversation_id
    # invariant:
    #   - graph contract does not change due to the selected runtime driver
    # modifies (internal):
    #   - external.openhands_server
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - RuntimeError: HTTP execution or payload parsing failed
    # depends:
    #   - OpenHandsHttpApi
    # sft: execute a task unit step through OpenHands agent server and parse the YAML result
    # idempotent: false
    # logs: query: OpenHands conversation events and execution status
    def run_task(self, request: DriverRequest) -> DriverResult:
        trace_id = ensure_trace_id(request.metadata.get("trace_id"))

        logger.info(
            "[OpenHandsDriver][run_task][ContextAnchor] trace_id=%s | "
            "Running driver task. phase=%s, role_dir=%s, sub_role=%s, model=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            request.model,
        )

        # === PRE[0]: request.prompt not empty ===
        logger.info(
            "[OpenHandsDriver][run_task][PreCheck] trace_id=%s | "
            "Checking request.prompt is not empty. phase=%s, role_dir=%s, sub_role=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
        )
        if not request.prompt.strip():
            logger.warning(
                "[OpenHandsDriver][run_task][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
                "Empty prompt for driver request. phase=%s, role_dir=%s, sub_role=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
            )
            raise RuntimeError("OpenHandsDriver received an empty prompt")

        payload = {
            "agent": {
                "discriminator": "Agent",
                "llm": {
                    "model": request.model,
                    "api_key": self._llm_api_key,
                    "base_url": self._llm_base_url,
                },
                "tools": [{"name": tool_name} for tool_name in self._tools],
                "cli_mode": self._cli_mode,
            },
            "workspace": {"working_dir": request.workspace_root},
            "initial_message": {
                "role": "user",
                "content": [{"type": "text", "text": request.prompt}],
                "run": True,
            },
        }

        try:
            handle = self._api.create_conversation(payload, trace_id=trace_id)
            initial_status = str(handle.state.get("execution_status", "")).upper()
            if initial_status in {"", "IDLE"}:
                self._api.run_conversation(handle.conversation_id, trace_id=trace_id)
            state = self._api.wait_until_finished(handle.conversation_id, trace_id=trace_id)
            events = self._api.search_events(
                handle.conversation_id,
                limit=OPENHANDS_EVENT_SEARCH_LIMIT_MAX,
                trace_id=trace_id,
            )
            text_fragments = _extract_texts({"state": state, "events": events})
            raw_text = "\n\n".join(text_fragments).strip()
            parsed_payload = _coerce_payload(raw_text)
            status = PipelineStatus(str(parsed_payload.get("status") or PipelineStatus.PASS))
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[OpenHandsDriver][run_task][ErrorHandled][ERR:EXTERNAL] trace_id=%s | "
                "Driver execution failed. phase=%s, role_dir=%s, sub_role=%s, error=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                str(exc),
            )
            raise RuntimeError(f"OpenHands driver failed: {exc}") from exc

        if not raw_text:
            status = _status_for_parse_failure(request.sub_role)
            parsed_payload = {
                "status": status,
                "warnings": ["OpenHands returned no parseable text output"],
            }

        logger.info(
            "[OpenHandsDriver][run_task][StepComplete] trace_id=%s | "
            "Driver task completed. phase=%s, role_dir=%s, sub_role=%s, status=%s, conversation_id=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            status,
            handle.conversation_id,
        )
        return DriverResult(
            status=status,
            payload=parsed_payload,
            raw_text=raw_text,
            conversation_id=handle.conversation_id,
        )

    # SEM_END orchestrator_v1.openhands_driver.openhands_driver.run_task:v1


# SEM_END orchestrator_v1.openhands_driver.openhands_driver:v1
