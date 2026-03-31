"""Low-level HTTP client for the OpenHands agent server."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from workflow_runtime.integrations.openhands_runtime import (
    OPENHANDS_EVENT_SEARCH_LIMIT_MAX,
)
from workflow_runtime.integrations.observability import ensure_trace_id


logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = {"FINISHED", "COMPLETED", "DONE", "PAUSED", "FAILED", "ERROR", "CANCELLED"}


@dataclass(frozen=True, slots=True)
class OpenHandsConversationHandle:
    conversation_id: str
    state: dict[str, Any]


class OpenHandsHttpApi:
    def __init__(self, base_url: str, timeout_seconds: int = 180, poll_interval_seconds: int = 2) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._client = httpx.Client(base_url=self._base_url, timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def _request(self, method: str, path: str, *, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self._client.request(method, path, json=json_body)
        response.raise_for_status()
        return response.json() if response.content else {}

    # SEM_BEGIN orchestrator_v1.openhands_http_api.create_conversation:v1
    # type: METHOD
# use_case: Creates a new OpenHands conversation through the REST API.
# feature:
#   - Phase 3 runtime path must launch the real OpenHands agent server
    #   - openhands/software-agent-sdk REST API
# pre:
#   - payload contains agent, workspace, and initial_message
# post:
#   - returns a handle with conversation_id and initial state
# invariant:
#   - HTTP client remains open after the call
    # modifies (internal):
    #   - external.openhands_server
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - httpx.HTTPError: OpenHands server request failed
    # depends:
    #   - httpx.Client
    # sft: create an OpenHands conversation through the REST API and return its id
    # idempotent: false
    # logs: command: uv run pytest tests/ -v | query: POST /api/conversations
    def create_conversation(self, payload: dict[str, Any], *, trace_id: str | None = None) -> OpenHandsConversationHandle:
        resolved_trace_id = ensure_trace_id(trace_id)
        logger.info(
            "[OpenHandsHttpApi][create_conversation][ExternalCall][BELIEF] trace_id=%s | "
            "Expecting conversation creation. path=/api/conversations",
            resolved_trace_id,
        )
        response = self._request("POST", "/api/conversations", json_body=payload)
        logger.info(
            "[OpenHandsHttpApi][create_conversation][ExternalCall][GROUND] trace_id=%s | "
            "Conversation created. status_code=200, conversation_id=%s",
            resolved_trace_id,
            response.get("conversation_id") or response.get("id"),
        )
        return OpenHandsConversationHandle(
            conversation_id=str(response.get("conversation_id") or response["id"]),
            state=response,
        )

    # SEM_END orchestrator_v1.openhands_http_api.create_conversation:v1

    def send_message(
        self,
        conversation_id: str,
        payload: dict[str, Any],
        *,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        logger.info(
            "[OpenHandsHttpApi][send_message][ExternalCall][BELIEF] trace_id=%s | "
            "Expecting event append. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        response = self._request(
            "POST",
            f"/api/conversations/{conversation_id}/events",
            json_body=payload,
        )
        logger.info(
            "[OpenHandsHttpApi][send_message][ExternalCall][GROUND] trace_id=%s | "
            "Event appended. status_code=200, conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        return response

    def run_conversation(self, conversation_id: str, *, trace_id: str | None = None) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        logger.info(
            "[OpenHandsHttpApi][run_conversation][ExternalCall][BELIEF] trace_id=%s | "
            "Expecting remote run. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        response = self._request("POST", f"/api/conversations/{conversation_id}/run")
        logger.info(
            "[OpenHandsHttpApi][run_conversation][ExternalCall][GROUND] trace_id=%s | "
            "Run triggered. status_code=200, conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        return response

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/conversations/{conversation_id}")

    def search_events(self, conversation_id: str, limit: int = 100) -> dict[str, Any]:
        safe_limit = min(limit, OPENHANDS_EVENT_SEARCH_LIMIT_MAX)
        return self._request("GET", f"/api/conversations/{conversation_id}/events/search?limit={safe_limit}")

    # SEM_BEGIN orchestrator_v1.openhands_http_api.wait_until_finished:v1
    # type: METHOD
# use_case: Polls an OpenHands conversation until it reaches a terminal status.
# feature:
#   - Phase 3 driver must wait for the remote run to complete before parsing the result
    #   - openhands/software-agent-sdk REST API
# pre:
#   - conversation_id is not empty
# post:
#   - returns the latest conversation state from the OpenHands server
# invariant:
#   - client reuse is preserved
    # modifies (internal):
    #   - external.openhands_server
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - TimeoutError: wait timeout exceeded
    # depends:
    #   - get_conversation
    # sft: wait for an OpenHands conversation to reach a terminal execution status
    # idempotent: false
    # logs: query: GET /api/conversations/{id}
    def wait_until_finished(self, conversation_id: str, *, trace_id: str | None = None) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        deadline = time.monotonic() + self._timeout_seconds

        logger.info(
            "[OpenHandsHttpApi][wait_until_finished][ContextAnchor] trace_id=%s | "
            "Waiting for conversation. conversation_id=%s, timeout_seconds=%d",
            resolved_trace_id,
            conversation_id,
            self._timeout_seconds,
        )

        while time.monotonic() < deadline:
            state = self.get_conversation(conversation_id)
            execution_status = str(
                state.get("execution_status")
                or state.get("status")
                or state.get("state", {}).get("execution_status", "")
            ).upper()

            logger.info(
                "[OpenHandsHttpApi][wait_until_finished][DecisionPoint] trace_id=%s | "
                "Branch: poll_status. Reason: conversation_id=%s, execution_status=%s",
                resolved_trace_id,
                conversation_id,
                execution_status,
            )

            if execution_status in _TERMINAL_STATUSES:
                logger.info(
                    "[OpenHandsHttpApi][wait_until_finished][StepComplete] trace_id=%s | "
                    "Conversation reached terminal state. conversation_id=%s, execution_status=%s",
                    resolved_trace_id,
                    conversation_id,
                    execution_status,
                )
                return state

            time.sleep(self._poll_interval_seconds)

        logger.error(
            "[OpenHandsHttpApi][wait_until_finished][ErrorHandled][ERR:TIMEOUT] trace_id=%s | "
            "Timed out waiting for conversation. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        raise TimeoutError(f"Timed out waiting for OpenHands conversation '{conversation_id}'")

    # SEM_END orchestrator_v1.openhands_http_api.wait_until_finished:v1
