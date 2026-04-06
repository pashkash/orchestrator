"""Low-level HTTP client for the OpenHands agent server."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx
from opentelemetry.propagate import inject as otel_inject

from workflow_runtime.integrations.openhands_runtime import (
    OPENHANDS_EVENT_SEARCH_LIMIT_MAX,
    OPENHANDS_TERMINAL_EXECUTION_STATUSES,
    normalize_openhands_execution_status,
)
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger


logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_conversation_handle:v1
# type: CLASS
# use_case: Immutable pointer to one OpenHands conversation and its latest known state.
# feature:
#   - Driver and polling code exchange a stable handle after conversation creation
#   - Phase 3 runtime integration uses the REST API conversation id as the durable reference
# pre:
#   -
# post:
#   -
# invariant:
#   - conversation_id uniquely identifies one remote execution thread
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: define immutable OpenHands conversation handle with id and latest state snapshot
# idempotent: -
# logs: -
@dataclass(frozen=True, slots=True)
class OpenHandsConversationHandle:
    conversation_id: str
    state: dict[str, Any]


# SEM_END orchestrator_v1.openhands_http_api.openhands_conversation_handle:v1


# SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api:v1
# type: CLASS
# use_case: Thin REST client around the OpenHands agent server.
# feature:
#   - The runtime driver must create conversations trigger runs fetch state and read events through one reusable client
#   - Task card 2026-03-24_1800__multi-agent-system-design, D5
# pre:
#   -
# post:
#   -
# invariant:
#   - one reusable httpx client is kept for the lifetime of the API object
# modifies (internal):
#   -
# emits (external):
#   - external.openhands_server
# errors:
#   - httpx.HTTPError: remote API request failed
# depends:
#   - httpx.Client
# sft: implement reusable REST client for OpenHands conversation lifecycle and event polling
# idempotent: false
# logs: command: uv run pytest tests/ -v | query: OpenHandsHttpApi trace_id
class OpenHandsHttpApi:
    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api.__init__:v1
    # type: METHOD
    # use_case: Creates a reusable HTTP client for the OpenHands REST API.
    # feature:
    #   - Runtime driver should reuse one client across conversation lifecycle calls to keep transport setup centralized
    # pre:
    #   - base_url is not empty
    # post:
    #   - stores sanitized base URL timeout poll interval and initialized httpx client
    # invariant:
    #   - base_url is stored without a trailing slash
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   - httpx.HTTPError: client setup failed
    # depends:
    #   - httpx.Client
    # sft: initialize reusable OpenHands HTTP API client with base url timeout and poll interval
    # idempotent: false
    # logs: -
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 180,
        poll_interval_seconds: int = 2,
        max_poll_interval_seconds: int | None = None,
        poll_log_every_n_attempts: int | None = None,
    ) -> None:
        if max_poll_interval_seconds is None:
            raise ValueError("OpenHandsHttpApi requires max_poll_interval_seconds from runtime config")
        if poll_log_every_n_attempts is None:
            raise ValueError("OpenHandsHttpApi requires poll_log_every_n_attempts from runtime config")
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._max_poll_interval_seconds = max_poll_interval_seconds
        self._poll_log_every_n_attempts = poll_log_every_n_attempts
        self._client = httpx.Client(base_url=self._base_url, timeout=timeout_seconds)

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api.__init__:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api.close:v1
    # type: METHOD
    # use_case: Closes the underlying HTTP client when the runtime backend is shutting down.
    # feature:
    #   - Reusable driver infrastructure should release transport resources explicitly
    # pre:
    #   -
    # post:
    #   - underlying httpx client is closed
    # invariant:
    #   - no OpenHands state is mutated by client shutdown
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   - httpx.HTTPError: client close failed
    # depends:
    #   - httpx.Client
    # sft: close the reusable OpenHands HTTP client when runtime execution is done
    # idempotent: false
    # logs: -
    def close(self) -> None:
        self._client.close()

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api.close:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api._request:v1
    # type: METHOD
    # use_case: Executes one low-level HTTP request against the OpenHands REST API and returns parsed JSON.
    # feature:
    #   - Centralizes transport-level status validation for all conversation lifecycle calls
    # pre:
    #   - method and path describe a valid OpenHands REST endpoint
    # post:
    #   - returns parsed JSON or an empty dict for empty responses
    # invariant:
    #   - the shared httpx client stays open after the request
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - httpx.HTTPError: request failed or returned non-success status
    # depends:
    #   - httpx.Client
    # sft: execute one OpenHands REST request and normalize the response body into a dict
    # idempotent: false
    # logs: query: OpenHandsHttpApi _request path
    def _request(self, method: str, path: str, *, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers: dict[str, str] = {}
        otel_inject(headers)
        try:
            from lmnr import Laminar
            ctx_str = Laminar.serialize_span_context()
            if ctx_str:
                headers["x-lmnr-parent-ctx"] = ctx_str
        except Exception:
            pass
        response = self._client.request(method, path, json=json_body, headers=headers)
        response.raise_for_status()
        return response.json() if response.content else {}

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api._request:v1

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
            "[OpenHandsHttpApi][create_conversation][ContextAnchor] trace_id=%s | "
            "Creating conversation. path=/api/conversations",
            resolved_trace_id,
        )
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
        handle = OpenHandsConversationHandle(
            conversation_id=str(response.get("conversation_id") or response["id"]),
            state=response,
        )
        logger.info(
            "[OpenHandsHttpApi][create_conversation][StepComplete] trace_id=%s | "
            "Conversation handle created. conversation_id=%s",
            resolved_trace_id,
            handle.conversation_id,
        )
        return handle

    # SEM_END orchestrator_v1.openhands_http_api.create_conversation:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api.send_message:v1
    # type: METHOD
    # use_case: Appends one event or message into an existing OpenHands conversation.
    # feature:
    #   - Driver-side follow-up messages must reuse the same remote conversation context
    # pre:
    #   - conversation_id is not empty
    #   - payload is a valid OpenHands event payload
    # post:
    #   - returns the API response for the appended event
    # invariant:
    #   - conversation_id does not change during the call
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - httpx.HTTPError: remote API request failed
    # depends:
    #   - _request
    # sft: append a message event to an existing OpenHands conversation and return the API response
    # idempotent: false
    # logs: query: POST /api/conversations/{id}/events
    def send_message(
        self,
        conversation_id: str,
        payload: dict[str, Any],
        *,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        logger.info(
            "[OpenHandsHttpApi][send_message][ContextAnchor] trace_id=%s | "
            "Sending conversation event. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
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
        logger.info(
            "[OpenHandsHttpApi][send_message][StepComplete] trace_id=%s | "
            "Conversation event sent. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        return response

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api.send_message:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api.run_conversation:v1
    # type: METHOD
    # use_case: Triggers execution for an already-created OpenHands conversation.
    # feature:
    #   - Some conversations start in IDLE state and must be explicitly started by the runtime driver
    # pre:
    #   - conversation_id is not empty
    # post:
    #   - returns the API response from the run trigger endpoint
    # invariant:
    #   - the OpenHands conversation id stays unchanged
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - httpx.HTTPError: remote API request failed
    # depends:
    #   - _request
    # sft: trigger execution for an existing OpenHands conversation through the run endpoint
    # idempotent: false
    # logs: query: POST /api/conversations/{id}/run
    def run_conversation(self, conversation_id: str, *, trace_id: str | None = None) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        logger.info(
            "[OpenHandsHttpApi][run_conversation][ContextAnchor] trace_id=%s | "
            "Triggering conversation run. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        logger.info(
            "[OpenHandsHttpApi][run_conversation][ExternalCall][BELIEF] trace_id=%s | "
            "Expecting remote run. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        try:
            response = self._request("POST", f"/api/conversations/{conversation_id}/run")
        except Exception as exc:
            if "409" in str(exc):
                logger.info(
                    "[OpenHandsHttpApi][run_conversation][DecisionPoint] trace_id=%s | "
                    "Branch: already_running. Reason: 409 Conflict (conversation already started). "
                    "conversation_id=%s",
                    resolved_trace_id,
                    conversation_id,
                )
                return {"already_running": True}
            raise
        logger.info(
            "[OpenHandsHttpApi][run_conversation][ExternalCall][GROUND] trace_id=%s | "
            "Run triggered. status_code=200, conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        logger.info(
            "[OpenHandsHttpApi][run_conversation][StepComplete] trace_id=%s | "
            "Conversation run triggered. conversation_id=%s",
            resolved_trace_id,
            conversation_id,
        )
        return response

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api.run_conversation:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api.get_conversation:v1
    # type: METHOD
    # use_case: Fetches the latest state of one OpenHands conversation.
    # feature:
    #   - Driver polling reads execution_status from the authoritative conversation state endpoint
    # pre:
    #   - conversation_id is not empty
    # post:
    #   - returns the current conversation state payload
    # invariant:
    #   - remote conversation is not mutated by the read call
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - httpx.HTTPError: remote API request failed
    # depends:
    #   - _request
    # sft: fetch the latest OpenHands conversation state by id
    # idempotent: true
    # logs: query: GET /api/conversations/{id}
    def get_conversation(
        self,
        conversation_id: str,
        *,
        trace_id: str | None = None,
        log_reads: bool = True,
    ) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        if log_reads:
            logger.info(
                "[OpenHandsHttpApi][get_conversation][ContextAnchor] trace_id=%s | "
                "Fetching conversation state. conversation_id=%s",
                resolved_trace_id,
                conversation_id,
            )
            logger.info(
                "[OpenHandsHttpApi][get_conversation][ExternalCall][BELIEF] trace_id=%s | "
                "Expecting conversation state. conversation_id=%s",
                resolved_trace_id,
                conversation_id,
            )
        response = self._request("GET", f"/api/conversations/{conversation_id}")
        if log_reads:
            logger.info(
                "[OpenHandsHttpApi][get_conversation][ExternalCall][GROUND] trace_id=%s | "
                "Conversation state received. status_code=200, conversation_id=%s",
                resolved_trace_id,
                conversation_id,
            )
            logger.info(
                "[OpenHandsHttpApi][get_conversation][StepComplete] trace_id=%s | "
                "Fetched conversation state. conversation_id=%s",
                resolved_trace_id,
                conversation_id,
            )
        return response

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api.get_conversation:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.openhands_http_api.search_events:v1
    # type: METHOD
    # use_case: Reads conversation events from OpenHands with a server-safe capped limit.
    # feature:
    #   - OpenHands v1.16 enforces an upper event-search limit and the runtime must clamp requests to that value
    # pre:
    #   - conversation_id is not empty
    # post:
    #   - returns the event-search payload using a capped limit
    # invariant:
    #   - requested limit never exceeds OPENHANDS_EVENT_SEARCH_LIMIT_MAX
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - httpx.HTTPError: remote API request failed
    # depends:
    #   - OPENHANDS_EVENT_SEARCH_LIMIT_MAX
    #   - _request
    # sft: fetch OpenHands conversation events while clamping the query limit to the server maximum
    # idempotent: true
    # logs: query: GET /api/conversations/{id}/events/search
    def search_events(self, conversation_id: str, limit: int = 100, *, trace_id: str | None = None) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        safe_limit = min(limit, OPENHANDS_EVENT_SEARCH_LIMIT_MAX)
        logger.info(
            "[OpenHandsHttpApi][search_events][ContextAnchor] trace_id=%s | "
            "Fetching conversation events. conversation_id=%s, requested_limit=%d, safe_limit=%d",
            resolved_trace_id,
            conversation_id,
            limit,
            safe_limit,
        )
        logger.info(
            "[OpenHandsHttpApi][search_events][ExternalCall][BELIEF] trace_id=%s | "
            "Expecting conversation events. conversation_id=%s, safe_limit=%d",
            resolved_trace_id,
            conversation_id,
            safe_limit,
        )
        response = self._request("GET", f"/api/conversations/{conversation_id}/events/search?limit={safe_limit}")
        logger.info(
            "[OpenHandsHttpApi][search_events][ExternalCall][GROUND] trace_id=%s | "
            "Conversation events received. status_code=200, conversation_id=%s, safe_limit=%d",
            resolved_trace_id,
            conversation_id,
            safe_limit,
        )
        logger.info(
            "[OpenHandsHttpApi][search_events][StepComplete] trace_id=%s | "
            "Fetched conversation events. conversation_id=%s, safe_limit=%d",
            resolved_trace_id,
            conversation_id,
            safe_limit,
        )
        return response

    # SEM_END orchestrator_v1.openhands_http_api.openhands_http_api.search_events:v1

    # SEM_BEGIN orchestrator_v1.openhands_http_api.wait_until_finished:v1
    # type: METHOD
    # use_case: Polls an OpenHands conversation until it reaches a terminal status without aborting while progress continues.
    # feature:
    #   - Phase 3 driver must wait for the remote run to complete before parsing the result
    #   - Long-running conversations must stay alive while execution_status or updated_at still change
    #   - openhands/software-agent-sdk REST API
    # pre:
    #   - conversation_id is not empty
    # post:
    #   - returns the latest terminal conversation state from the OpenHands server
    # invariant:
    #   - client reuse is preserved
    # modifies (internal):
    #   - external.openhands_server
    # emits (external):
    #   - external.openhands_server
    # errors:
    #   - TimeoutError: idle timeout exceeded without conversation progress
    # depends:
    #   - get_conversation
    # sft: wait for an OpenHands conversation to reach a terminal execution status using idle-timeout semantics
    # idempotent: false
    # logs: query: GET /api/conversations/{id}
    def wait_until_finished(self, conversation_id: str, *, trace_id: str | None = None) -> dict[str, Any]:
        resolved_trace_id = ensure_trace_id(trace_id)
        current_monotonic = time.monotonic()
        idle_deadline = current_monotonic + self._timeout_seconds
        poll_attempt = 0
        sleep_seconds = max(1, self._poll_interval_seconds)
        last_execution_status: str | None = None
        last_updated_at: str | None = None
        started_wait_at = current_monotonic
        last_progress_at = current_monotonic

        logger.info(
            "[OpenHandsHttpApi][wait_until_finished][ContextAnchor] trace_id=%s | "
            "Waiting for conversation. conversation_id=%s, idle_timeout_seconds=%d",
            resolved_trace_id,
            conversation_id,
            self._timeout_seconds,
        )

        while True:
            poll_attempt += 1
            state = self.get_conversation(
                conversation_id,
                trace_id=resolved_trace_id,
                log_reads=False,
            )
            execution_status = str(
                state.get("execution_status")
                or state.get("status")
                or state.get("state", {}).get("execution_status", "")
            ).upper()
            updated_at = str(
                state.get("updated_at")
                or state.get("state", {}).get("updated_at")
                or ""
            )

            should_log_poll = (
                poll_attempt == 1
                or execution_status != last_execution_status
                or updated_at != last_updated_at
                or poll_attempt % self._poll_log_every_n_attempts == 0
            )
            if should_log_poll:
                logger.info(
                    "[OpenHandsHttpApi][wait_until_finished][DecisionPoint] trace_id=%s | "
                    "Branch: poll_status. Reason: conversation_id=%s, execution_status=%s, "
                    "poll_attempt=%d, next_sleep_seconds=%d",
                    resolved_trace_id,
                    conversation_id,
                    execution_status,
                    poll_attempt,
                    sleep_seconds,
                )

            progress_detected = (
                poll_attempt == 1
                or execution_status != last_execution_status
                or (updated_at and updated_at != last_updated_at)
            )
            if progress_detected:
                last_progress_at = time.monotonic()
                idle_deadline = last_progress_at + self._timeout_seconds

            last_execution_status = execution_status
            last_updated_at = updated_at

            normalized_execution_status = normalize_openhands_execution_status(execution_status)
            if normalized_execution_status in OPENHANDS_TERMINAL_EXECUTION_STATUSES:
                logger.info(
                    "[OpenHandsHttpApi][wait_until_finished][StepComplete] trace_id=%s | "
                    "Conversation reached terminal state. conversation_id=%s, execution_status=%s, "
                    "poll_attempts=%d",
                    resolved_trace_id,
                    conversation_id,
                    execution_status,
                    poll_attempt,
                )
                return state

            if time.monotonic() >= idle_deadline:
                idle_for_seconds = int(time.monotonic() - last_progress_at)
                total_wait_seconds = int(time.monotonic() - started_wait_at)
                logger.error(
                    "[OpenHandsHttpApi][wait_until_finished][ErrorHandled][ERR:TIMEOUT] trace_id=%s | "
                    "Timed out waiting for conversation progress. conversation_id=%s, poll_attempts=%d, "
                    "execution_status=%s, idle_for_seconds=%d, total_wait_seconds=%d",
                    resolved_trace_id,
                    conversation_id,
                    poll_attempt,
                    execution_status,
                    idle_for_seconds,
                    total_wait_seconds,
                )
                raise TimeoutError(
                    f"Timed out waiting for OpenHands conversation progress '{conversation_id}'"
                )

            time.sleep(sleep_seconds)
            sleep_seconds = min(sleep_seconds * 2, self._max_poll_interval_seconds)

    # SEM_END orchestrator_v1.openhands_http_api.wait_until_finished:v1


# SEM_END orchestrator_v1.openhands_http_api.openhands_http_api:v1
