"""OpenHands-backed runtime driver."""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml

from squadder_orchestrator.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from squadder_orchestrator.graph_compiler.state_schema import PipelineStatus, SubRole
from squadder_orchestrator.integrations.observability import ensure_trace_id
from squadder_orchestrator.integrations.openhands_http_api import OpenHandsHttpApi
from squadder_orchestrator.integrations.openhands_runtime import (
    OPENHANDS_EVENT_SEARCH_LIMIT_MAX,
)


logger = logging.getLogger(__name__)

_YAML_BLOCK_RE = re.compile(r"```(?:yaml|yml)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


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


def _status_for_parse_failure(sub_role: SubRole) -> PipelineStatus:
    if sub_role == SubRole.REVIEWER:
        return PipelineStatus.NEEDS_FIX_REVIEW
    if sub_role == SubRole.TESTER:
        return PipelineStatus.NEEDS_FIX_TESTS
    return PipelineStatus.NEEDS_FIX_EXECUTOR


class OpenHandsDriver(BaseDriver):
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

    # SEM_BEGIN orchestrator_v1.openhands_driver.run_task:v1
    # type: METHOD
    # use_case: Запускает один шаг TaskUnit через реальный OpenHands agent server.
    # feature:
    #   - Phase 3 использует тот же TaskUnit contract, но backend исполнения уже реальный
    #   - orchestrator/config/phases_and_roles.yaml -> runtime.openhands + per-step model
    # pre:
    #   - request.prompt не пустой
    #   - llm_api_key сконфигурирован
    # post:
    #   - возвращает DriverResult со status, payload и conversation_id
    # invariant:
    #   - graph contract не меняется из-за выбранного runtime driver
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

    # SEM_END orchestrator_v1.openhands_driver.run_task:v1
