"""Single-shot direct LLM runtime driver."""

from __future__ import annotations

import threading
import time
from queue import Empty, Queue
from typing import Any, Callable

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from lmnr import Laminar, observe

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.agent_drivers.yaml_contract import (
    coerce_payload,
    extract_text_content,
    missing_required_payload_keys,
    normalize_payload_shape,
    normalize_status,
    resolve_provider_model_name,
    status_for_parse_failure,
)
from workflow_runtime.integrations.observability import ensure_trace_id
from workflow_runtime.integrations.runtime_logging import get_logger

logger = get_logger(__name__)


# SEM_BEGIN orchestrator_v1.direct_llm_driver.timeout_error:v1
# type: CLASS
# use_case: Инкапсулирует hard/idle timeout outcome для direct-LLM attempt и передаёт диагностический контекст в retry/logging path.
# feature:
#   - Runtime должен различать "вообще не закончилось вовремя" и "15 секунд не было новых токенов"
#   - orchestrator/config/phases_and_roles.yaml -> runtime.direct_llm.idle_timeout_seconds
# pre:
#   - timeout_kind in {"hard", "idle"}
# post:
#   - exception содержит timeout kind, elapsed seconds и stream-progress markers
# invariant:
#   - исходный timeout outcome не мутируется после создания экземпляра
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   -
# sft: represent direct llm hard and idle timeout outcomes with diagnostic metadata
# idempotent: true
# logs: query: DirectLlmDriver direct_llm_timeout
class _DirectLlmTimeoutError(TimeoutError):
    def __init__(
        self,
        *,
        timeout_kind: str,
        timeout_seconds: int,
        elapsed_seconds: float,
        saw_output: bool,
        chunk_count: int,
    ) -> None:
        self.timeout_kind = timeout_kind
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds
        self.saw_output = saw_output
        self.chunk_count = chunk_count
        if timeout_kind == "idle":
            message = (
                f"Direct LLM provider idle timeout exceeded {timeout_seconds}s "
                f"without streamed output progress (elapsed={elapsed_seconds:.1f}s)"
            )
        else:
            message = (
                f"Direct LLM provider watchdog exceeded {timeout_seconds}s "
                f"(elapsed={elapsed_seconds:.1f}s)"
            )
        super().__init__(message)
# SEM_END orchestrator_v1.direct_llm_driver.timeout_error:v1


# SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver:v1
# type: CLASS
# use_case: Выполняет один runtime step через прямой single-shot LLM call без OpenHands.
# feature:
#   - Нужен для planner/reviewer/validate шагов, где tools не обязательны и OH loop даёт лишний overhead
#   - Task card 2026-04-05_1900__oh-laminar-otel-gui, T43
# pre:
#   -
# post:
#   -
# invariant:
#   - driver возвращает тот же DriverResult contract, что и остальные backends
# modifies (internal):
#   -
# emits (external):
#   - external.llm_provider
# errors:
#   - RuntimeError: LLM call or YAML parsing failed
# depends:
#   - ChatOpenAI
# sft: execute one task unit step through a direct single-shot LLM call and parse the YAML response
# idempotent: false
# logs: query: DirectLlmDriver trace_id
class DirectLlmDriver(BaseDriver):
    # SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver.__init__:v1
    # type: METHOD
    # use_case: Сохраняет конфигурацию прямого LLM backend для следующих шагов runtime.
    # feature:
    #   - Runtime graph reuse-ит одну driver-конфигурацию для planner/reviewer/validate шагов
    # pre:
    #   -
    # post:
    #   - driver хранит api key, base url и timeout
    # invariant:
    #   - конфигурация backend не мутируется между вызовами run_task
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   -
    # depends:
    #   - ChatOpenAI
    # sft: initialize direct llm runtime driver with API key base url and timeout settings
    # idempotent: false
    # logs: -
    def __init__(
        self,
        *,
        llm_api_key: str | None,
        llm_base_url: str,
        timeout_seconds: int,
        max_attempts: int,
        retry_backoff_seconds: int,
        idle_timeout_seconds: int = 15,
    ) -> None:
        self._llm_api_key = llm_api_key
        self._llm_base_url = llm_base_url
        self._timeout_seconds = timeout_seconds
        self._idle_timeout_seconds = idle_timeout_seconds
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds

    # SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver.__init__:v1

    def _build_llm(self, *, model: str, timeout_seconds: int | None = None) -> ChatOpenAI:
        return ChatOpenAI(
            model=resolve_provider_model_name(model, self._llm_base_url),
            api_key=self._llm_api_key,
            base_url=self._llm_base_url,
            timeout=timeout_seconds or self._timeout_seconds,
            temperature=0,
        )

    def _resolve_timeout_settings(
        self,
        *,
        request: DriverRequest,
        messages: list[Any],
        trace_id: str,
    ) -> tuple[int, int]:
        hard_timeout_seconds = self._timeout_seconds
        idle_timeout_seconds = self._idle_timeout_seconds
        runtime_overrides = request.metadata.get("execution_runtime_overrides")
        if isinstance(runtime_overrides, dict):
            raw_hard_timeout = runtime_overrides.get("timeout_seconds")
            raw_idle_timeout = runtime_overrides.get("idle_timeout_seconds")
            if raw_hard_timeout is not None:
                try:
                    hard_timeout_seconds = max(1, int(raw_hard_timeout))
                except (TypeError, ValueError):
                    logger.warning(
                        "[DirectLlmDriver][_resolve_timeout_settings][ErrorHandled][ERR:DATA_INTEGRITY] trace_id=%s | "
                        "Ignoring invalid hard timeout override. phase=%s, role_dir=%s, sub_role=%s, value=%r",
                        trace_id,
                        request.phase_id,
                        request.role_dir,
                        request.sub_role,
                        raw_hard_timeout,
                    )
            if raw_idle_timeout is not None:
                try:
                    idle_timeout_seconds = max(1, int(raw_idle_timeout))
                except (TypeError, ValueError):
                    logger.warning(
                        "[DirectLlmDriver][_resolve_timeout_settings][ErrorHandled][ERR:DATA_INTEGRITY] trace_id=%s | "
                        "Ignoring invalid idle timeout override. phase=%s, role_dir=%s, sub_role=%s, value=%r",
                        trace_id,
                        request.phase_id,
                        request.role_dir,
                        request.sub_role,
                        raw_idle_timeout,
                    )
        prompt_chars = sum(len(str(getattr(message, "content", "") or "")) for message in messages)
        if prompt_chars >= 140000:
            hard_timeout_seconds = max(hard_timeout_seconds, 360)
            idle_timeout_seconds = max(idle_timeout_seconds, 60)
        elif prompt_chars >= 100000:
            hard_timeout_seconds = max(hard_timeout_seconds, 300)
            idle_timeout_seconds = max(idle_timeout_seconds, 45)
        elif prompt_chars >= 75000:
            hard_timeout_seconds = max(hard_timeout_seconds, 240)
            idle_timeout_seconds = max(idle_timeout_seconds, 30)
        if idle_timeout_seconds > hard_timeout_seconds:
            idle_timeout_seconds = hard_timeout_seconds
        logger.info(
            "[DirectLlmDriver][_resolve_timeout_settings][StepComplete] trace_id=%s | "
            "Resolved effective timeout settings. phase=%s, role_dir=%s, sub_role=%s, "
            "hard_timeout_seconds=%d, idle_timeout_seconds=%d, prompt_chars=%d, overrides_applied=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            hard_timeout_seconds,
            idle_timeout_seconds,
            prompt_chars,
            isinstance(runtime_overrides, dict) and bool(runtime_overrides),
        )
        return hard_timeout_seconds, idle_timeout_seconds

    # SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver._observe_timeout:v1
    # type: METHOD
    # use_case: Эмитит отдельный Laminar span для hard/idle timeout исхода provider-attempt.
    # feature:
    #   - timeout outcome должен быть виден в trace отдельно от generic provider error
    #   - UI/логика retry должны различать hard watchdog и idle timeout без новых токенов
    # pre:
    #   - timeout_error.timeout_kind in {"hard", "idle"}
    # post:
    #   - timeout span завершён с атрибутами kind/elapsed/chunk_count
    # invariant:
    #   - основной provider span не завершается внутри этого метода
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.llm_provider
    # errors:
    #   -
    # depends:
    #   - Laminar
    # sft: emit dedicated timeout span for direct llm hard or idle timeout
    # idempotent: false
    # logs: query: DirectLlmDriver _observe_timeout
    def _observe_timeout(
        self,
        *,
        trace_id: str,
        phase_id: str,
        role_dir: str,
        sub_role: str,
        call_kind: str,
        attempt: int,
        timeout_error: _DirectLlmTimeoutError,
    ) -> None:
        logger.error(
            "[DirectLlmDriver][_observe_timeout][ErrorHandled][ERR:TIMEOUT] trace_id=%s | "
            "Provider timeout observed. phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, "
            "attempt=%d/%d, timeout_kind=%s, timeout_seconds=%d, elapsed_seconds=%.3f, "
            "saw_output=%s, chunk_count=%d",
            trace_id,
            phase_id,
            role_dir,
            sub_role,
            call_kind,
            attempt,
            self._max_attempts,
            timeout_error.timeout_kind,
            timeout_error.timeout_seconds,
            timeout_error.elapsed_seconds,
            timeout_error.saw_output,
            timeout_error.chunk_count,
        )
        span = Laminar.start_active_span(
            "direct_llm_timeout",
            input={
                "phase": phase_id,
                "role_dir": role_dir,
                "sub_role": sub_role,
                "call_kind": call_kind,
                "attempt": attempt,
                "timeout_kind": timeout_error.timeout_kind,
            },
        )
        Laminar.set_span_attributes(
            {
                "direct_llm.trace_id": trace_id,
                "direct_llm.phase": phase_id,
                "direct_llm.role_dir": role_dir,
                "direct_llm.sub_role": sub_role,
                "direct_llm.call_kind": call_kind,
                "direct_llm.attempt": attempt,
                "direct_llm.timeout_kind": timeout_error.timeout_kind,
                "direct_llm.timeout_seconds": timeout_error.timeout_seconds,
                "direct_llm.elapsed_seconds": round(timeout_error.elapsed_seconds, 3),
                "direct_llm.saw_output": timeout_error.saw_output,
                "direct_llm.chunk_count": timeout_error.chunk_count,
            }
        )
        try:
            Laminar.set_span_output(
                {
                    "status": "timeout",
                    "timeout_kind": timeout_error.timeout_kind,
                    "attempt": attempt,
                    "elapsed_seconds": round(timeout_error.elapsed_seconds, 3),
                    "saw_output": timeout_error.saw_output,
                    "chunk_count": timeout_error.chunk_count,
                }
            )
        finally:
            span.end()
    # SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver._observe_timeout:v1

    # SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver._invoke_once_with_watchdog:v1
    # type: METHOD
    # use_case: Выполняет один provider-attempt через streaming path, контролируя общий hard timeout и idle timeout без новых chunk-ов.
    # feature:
    #   - Direct LLM step не должен зависать дольше runtime.direct_llm.timeout_seconds
    #   - Если provider молчит дольше runtime.direct_llm.idle_timeout_seconds, attempt нужно оборвать и ретраить
    # pre:
    #   - llm is not None
    #   - messages is not empty
    # post:
    #   - returns final model response and stream diagnostics on success
    # invariant:
    #   - provider worker thread запускается не более одного раза на attempt
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.llm_provider
    # errors:
    #   - _DirectLlmTimeoutError: hard or idle timeout exceeded
    #   - RuntimeError: stream completed without final response
    # depends:
    #   - ChatOpenAI.stream
    #   - ChatOpenAI.invoke
    # sft: execute one direct llm attempt with hard and idle timeout control using stream when available
    # idempotent: false
    # logs: query: DirectLlmDriver _invoke_once_with_watchdog
    def _invoke_once_with_watchdog(
        self,
        *,
        llm: ChatOpenAI,
        messages: list[Any],
        hard_timeout_seconds: int,
        idle_timeout_seconds: int,
        trace_id: str,
        phase_id: str,
        role_dir: str,
        sub_role: str,
        call_kind: str,
        attempt: int,
    ) -> tuple[Any, dict[str, Any]]:
        logger.info(
            "[DirectLlmDriver][_invoke_once_with_watchdog][ContextAnchor] trace_id=%s | "
            "Starting provider watchdog. phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, "
            "attempt=%d/%d, hard_timeout_seconds=%d, idle_timeout_seconds=%d",
            trace_id,
            phase_id,
            role_dir,
            sub_role,
            call_kind,
            attempt,
            self._max_attempts,
            hard_timeout_seconds,
            idle_timeout_seconds,
        )
        events: Queue[tuple[str, Any]] = Queue()

        def _target() -> None:
            try:
                if hasattr(llm, "stream"):
                    logger.info(
                        "[DirectLlmDriver][_invoke_once_with_watchdog][DecisionPoint] trace_id=%s | "
                        "Branch: streaming_path. Reason: ChatOpenAI.stream available. phase=%s, role_dir=%s, "
                        "sub_role=%s, call_kind=%s, attempt=%d/%d",
                        trace_id,
                        phase_id,
                        role_dir,
                        sub_role,
                        call_kind,
                        attempt,
                        self._max_attempts,
                    )
                    full_response = None
                    chunk_count = 0
                    for chunk in llm.stream(messages):
                        if chunk is None:
                            continue
                        chunk_count += 1
                        full_response = chunk if full_response is None else full_response + chunk
                        events.put(("chunk", chunk_count))
                    events.put(("done", full_response))
                    return
                logger.info(
                    "[DirectLlmDriver][_invoke_once_with_watchdog][DecisionPoint] trace_id=%s | "
                    "Branch: invoke_fallback. Reason: ChatOpenAI.stream unavailable. phase=%s, role_dir=%s, "
                    "sub_role=%s, call_kind=%s, attempt=%d/%d",
                    trace_id,
                    phase_id,
                    role_dir,
                    sub_role,
                    call_kind,
                    attempt,
                    self._max_attempts,
                )
                events.put(("done", llm.invoke(messages)))
            except Exception as exc:  # noqa: BLE001
                events.put(("error", exc))

        worker = threading.Thread(target=_target, daemon=True)
        worker.start()
        started_at = time.monotonic()
        last_progress_at = started_at
        saw_output = False
        chunk_count = 0

        while True:
            now = time.monotonic()
            elapsed_seconds = now - started_at
            remaining_hard_timeout = hard_timeout_seconds - elapsed_seconds
            if remaining_hard_timeout <= 0:
                logger.error(
                    "[DirectLlmDriver][_invoke_once_with_watchdog][ErrorHandled][ERR:TIMEOUT] trace_id=%s | "
                    "Hard timeout reached before provider completion. phase=%s, role_dir=%s, sub_role=%s, "
                    "call_kind=%s, attempt=%d/%d, elapsed_seconds=%.3f, saw_output=%s, chunk_count=%d",
                    trace_id,
                    phase_id,
                    role_dir,
                    sub_role,
                    call_kind,
                    attempt,
                    self._max_attempts,
                    elapsed_seconds,
                    saw_output,
                    chunk_count,
                )
                raise _DirectLlmTimeoutError(
                    timeout_kind="hard",
                    timeout_seconds=hard_timeout_seconds,
                    elapsed_seconds=elapsed_seconds,
                    saw_output=saw_output,
                    chunk_count=chunk_count,
                )
            try:
                event_kind, payload = events.get(
                    timeout=min(float(idle_timeout_seconds), remaining_hard_timeout)
                )
            except Empty:
                now = time.monotonic()
                elapsed_seconds = now - started_at
                if elapsed_seconds >= hard_timeout_seconds:
                    logger.error(
                        "[DirectLlmDriver][_invoke_once_with_watchdog][ErrorHandled][ERR:TIMEOUT] trace_id=%s | "
                        "Hard timeout reached while waiting for provider progress. phase=%s, role_dir=%s, "
                        "sub_role=%s, call_kind=%s, attempt=%d/%d, elapsed_seconds=%.3f, saw_output=%s, "
                        "chunk_count=%d",
                        trace_id,
                        phase_id,
                        role_dir,
                        sub_role,
                        call_kind,
                        attempt,
                        self._max_attempts,
                        elapsed_seconds,
                        saw_output,
                        chunk_count,
                    )
                    raise _DirectLlmTimeoutError(
                        timeout_kind="hard",
                        timeout_seconds=hard_timeout_seconds,
                        elapsed_seconds=elapsed_seconds,
                        saw_output=saw_output,
                        chunk_count=chunk_count,
                    )
                logger.error(
                    "[DirectLlmDriver][_invoke_once_with_watchdog][ErrorHandled][ERR:TIMEOUT] trace_id=%s | "
                    "Idle timeout reached without stream progress. phase=%s, role_dir=%s, sub_role=%s, "
                    "call_kind=%s, attempt=%d/%d, elapsed_seconds=%.3f, saw_output=%s, chunk_count=%d",
                    trace_id,
                    phase_id,
                    role_dir,
                    sub_role,
                    call_kind,
                    attempt,
                    self._max_attempts,
                    elapsed_seconds,
                    saw_output,
                    chunk_count,
                )
                raise _DirectLlmTimeoutError(
                    timeout_kind="idle",
                    timeout_seconds=idle_timeout_seconds,
                    elapsed_seconds=elapsed_seconds,
                    saw_output=saw_output,
                    chunk_count=chunk_count,
                )

            if event_kind == "chunk":
                saw_output = True
                chunk_count = int(payload)
                last_progress_at = time.monotonic()
                continue
            if event_kind == "error":
                raise payload
            if payload is None:
                logger.error(
                    "[DirectLlmDriver][_invoke_once_with_watchdog][ErrorHandled][ERR:UNEXPECTED] trace_id=%s | "
                    "Provider stream completed without final response. phase=%s, role_dir=%s, sub_role=%s, "
                    "call_kind=%s, attempt=%d/%d, chunk_count=%d",
                    trace_id,
                    phase_id,
                    role_dir,
                    sub_role,
                    call_kind,
                    attempt,
                    self._max_attempts,
                    chunk_count,
                )
                raise RuntimeError("Direct LLM stream completed without a final response")
            metrics = {
                "saw_output": saw_output,
                "chunk_count": chunk_count,
                "elapsed_seconds": round(time.monotonic() - started_at, 3),
                "idle_seconds_since_last_chunk": round(time.monotonic() - last_progress_at, 3),
            }
            logger.info(
                "[DirectLlmDriver][_invoke_once_with_watchdog][StepComplete] trace_id=%s | "
                "Provider watchdog completed. phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, "
                "attempt=%d/%d, saw_output=%s, chunk_count=%d, elapsed_seconds=%.3f",
                trace_id,
                phase_id,
                role_dir,
                sub_role,
                call_kind,
                attempt,
                self._max_attempts,
                metrics["saw_output"],
                metrics["chunk_count"],
                metrics["elapsed_seconds"],
            )
            return payload, metrics
    # SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver._invoke_once_with_watchdog:v1

    def _observe_provider_attempt(
        self,
        *,
        llm_factory: Callable[[], ChatOpenAI],
        messages: list[Any],
        hard_timeout_seconds: int,
        idle_timeout_seconds: int,
        trace_id: str,
        phase_id: str,
        role_dir: str,
        sub_role: str,
        call_kind: str,
        attempt: int,
    ):
        span = Laminar.start_active_span(
            "direct_llm_provider_attempt",
            input={
                "phase": phase_id,
                "role_dir": role_dir,
                "sub_role": sub_role,
                "call_kind": call_kind,
                "attempt": attempt,
                "max_attempts": self._max_attempts,
            },
            span_type="LLM",
        )
        Laminar.set_span_attributes(
            {
                "direct_llm.trace_id": trace_id,
                "direct_llm.phase": phase_id,
                "direct_llm.role_dir": role_dir,
                "direct_llm.sub_role": sub_role,
                "direct_llm.call_kind": call_kind,
                "direct_llm.attempt": attempt,
                "direct_llm.max_attempts": self._max_attempts,
                "direct_llm.timeout_seconds": hard_timeout_seconds,
                "direct_llm.idle_timeout_seconds": idle_timeout_seconds,
            }
        )
        try:
            response, metrics = self._invoke_once_with_watchdog(
                llm=llm_factory(),
                messages=messages,
                hard_timeout_seconds=hard_timeout_seconds,
                idle_timeout_seconds=idle_timeout_seconds,
                trace_id=trace_id,
                phase_id=phase_id,
                role_dir=role_dir,
                sub_role=sub_role,
                call_kind=call_kind,
                attempt=attempt,
            )
            Laminar.set_span_attributes(
                {
                    "direct_llm.attempt_outcome": "success",
                    "direct_llm.stream_saw_output": bool(metrics["saw_output"]),
                    "direct_llm.stream_chunk_count": int(metrics["chunk_count"]),
                    "direct_llm.elapsed_seconds": float(metrics["elapsed_seconds"]),
                }
            )
            Laminar.set_span_output(
                {
                    "status": "success",
                    "attempt": attempt,
                    "stream_saw_output": bool(metrics["saw_output"]),
                    "stream_chunk_count": int(metrics["chunk_count"]),
                    "elapsed_seconds": float(metrics["elapsed_seconds"]),
                }
            )
            return response
        except _DirectLlmTimeoutError as exc:
            self._observe_timeout(
                trace_id=trace_id,
                phase_id=phase_id,
                role_dir=role_dir,
                sub_role=sub_role,
                call_kind=call_kind,
                attempt=attempt,
                timeout_error=exc,
            )
            Laminar.set_span_attributes(
                {
                    "direct_llm.attempt_outcome": "error",
                    "direct_llm.retryable": True,
                    "direct_llm.error": str(exc),
                    "direct_llm.timeout_kind": exc.timeout_kind,
                }
            )
            Laminar.set_span_output(
                {
                    "status": "error",
                    "attempt": attempt,
                    "retryable": True,
                    "error": str(exc),
                    "timeout_kind": exc.timeout_kind,
                }
            )
            raise
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_retryable_provider_error(exc)
            Laminar.set_span_attributes(
                {
                    "direct_llm.attempt_outcome": "error",
                    "direct_llm.retryable": retryable,
                    "direct_llm.error": str(exc),
                }
            )
            Laminar.set_span_output(
                {
                    "status": "error",
                    "attempt": attempt,
                    "retryable": retryable,
                    "error": str(exc),
                }
            )
            raise
        finally:
            span.end()

    def _observe_retry_backoff(
        self,
        *,
        trace_id: str,
        phase_id: str,
        role_dir: str,
        sub_role: str,
        call_kind: str,
        attempt: int,
        sleep_seconds: int,
    ) -> None:
        if sleep_seconds <= 0:
            return
        span = Laminar.start_active_span(
            "direct_llm_retry_backoff",
            input={
                "phase": phase_id,
                "role_dir": role_dir,
                "sub_role": sub_role,
                "call_kind": call_kind,
                "attempt": attempt,
                "sleep_seconds": sleep_seconds,
            },
        )
        Laminar.set_span_attributes(
            {
                "direct_llm.trace_id": trace_id,
                "direct_llm.phase": phase_id,
                "direct_llm.role_dir": role_dir,
                "direct_llm.sub_role": sub_role,
                "direct_llm.call_kind": call_kind,
                "direct_llm.attempt": attempt,
                "direct_llm.backoff_seconds": sleep_seconds,
            }
        )
        try:
            time.sleep(sleep_seconds)
            Laminar.set_span_output(
                {
                    "status": "slept",
                    "attempt": attempt,
                    "sleep_seconds": sleep_seconds,
                }
            )
        finally:
            span.end()

    def _invoke_once(
        self,
        *,
        llm: ChatOpenAI,
        messages: list[Any],
    ):
        return llm.invoke(messages)

    # SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver._is_retryable_provider_error:v1
    # type: METHOD
    # use_case: Определяет, относится ли ошибка LLM provider к retryable timeout/network классу.
    # feature:
    #   - Runtime должен повторять только зависания и транзиентные external failures, а не любые логические ошибки
    # pre:
    #   - exc is not None
    # post:
    #   - returns True only for timeout/temporary provider failures
    # invariant:
    #   - exception object is not mutated
    # modifies (internal):
    #   -
    # emits (external):
    #   -
    # errors:
    #   -
    # depends:
    #   -
    # sft: classify provider exceptions into retryable timeout/network failures versus terminal errors
    # idempotent: true
    # logs: -
    def _is_retryable_provider_error(self, exc: Exception) -> bool:
        class_name = exc.__class__.__name__.lower()
        message = str(exc).lower()
        timeout_markers = ("timeout", "timed out", "read timed out", "api timeout")
        transient_markers = (
            "connection error",
            "api connection",
            "service unavailable",
            "rate limit",
            "too many requests",
            "bad gateway",
            "gateway timeout",
        )
        if "timeout" in class_name:
            return True
        if any(marker in message for marker in timeout_markers):
            return True
        if any(marker in class_name for marker in ("connection", "ratelimit", "serviceunavailable")):
            return True
        return any(marker in message for marker in transient_markers)

    # SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver._is_retryable_provider_error:v1

    # SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver._invoke_with_retry:v1
    # type: METHOD
    # use_case: Выполняет один direct-LLM provider call с retry по timeout и транзиентным external ошибкам.
    # feature:
    #   - planner/reviewer/validate шаги должны переживать подвисание OpenRouter без silent hang до бесконечности
    # pre:
    #   - self._max_attempts >= 1
    #   - messages is not empty
    # post:
    #   - returns provider response on success
    #   - raises terminal error after retry budget exhaustion
    # invariant:
    #   - retry budget bounded by config
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.llm_provider
    # errors:
    #   - RuntimeError: retry budget exhausted for retryable provider errors
    # depends:
    #   - ChatOpenAI.invoke
    # sft: execute one provider call with bounded retries and explicit timeout diagnostics in logs
    # idempotent: false
    # logs: query: DirectLlmDriver provider_attempt trace_id
    @observe(name="direct_llm_retry_loop")
    def _invoke_with_retry(
        self,
        *,
        llm_factory: Callable[[], ChatOpenAI],
        messages: list[Any],
        hard_timeout_seconds: int,
        idle_timeout_seconds: int,
        trace_id: str,
        phase_id: str,
        role_dir: str,
        sub_role: str,
        call_kind: str,
    ):
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            logger.info(
                "[DirectLlmDriver][_invoke_with_retry][ContextAnchor] trace_id=%s | "
                "Starting provider attempt. phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, attempt=%d/%d, timeout_seconds=%d",
                trace_id,
                phase_id,
                role_dir,
                sub_role,
                call_kind,
                attempt,
                self._max_attempts,
                hard_timeout_seconds,
            )
            try:
                response = self._observe_provider_attempt(
                    llm_factory=llm_factory,
                    messages=messages,
                    hard_timeout_seconds=hard_timeout_seconds,
                    idle_timeout_seconds=idle_timeout_seconds,
                    trace_id=trace_id,
                    phase_id=phase_id,
                    role_dir=role_dir,
                    sub_role=sub_role,
                    call_kind=call_kind,
                    attempt=attempt,
                )
                logger.info(
                    "[DirectLlmDriver][_invoke_with_retry][StepComplete] trace_id=%s | "
                    "Provider attempt completed. phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, attempt=%d/%d",
                    trace_id,
                    phase_id,
                    role_dir,
                    sub_role,
                    call_kind,
                    attempt,
                    self._max_attempts,
                )
                return response
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                retryable = self._is_retryable_provider_error(exc)
                is_last_attempt = attempt >= self._max_attempts
                if not retryable or is_last_attempt:
                    logger.error(
                        "[DirectLlmDriver][_invoke_with_retry][ErrorHandled][ERR:EXTERNAL] trace_id=%s | "
                        "Provider attempt failed. phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, attempt=%d/%d, retryable=%s, error=%s",
                        trace_id,
                        phase_id,
                        role_dir,
                        sub_role,
                        call_kind,
                        attempt,
                        self._max_attempts,
                        retryable,
                        str(exc),
                    )
                    break
                sleep_seconds = self._retry_backoff_seconds * attempt
                logger.warning(
                    "[DirectLlmDriver][_invoke_with_retry][DecisionPoint] trace_id=%s | "
                    "Branch: retry_provider_attempt. Reason: phase=%s, role_dir=%s, sub_role=%s, call_kind=%s, "
                    "attempt=%d/%d, error=%s, next_retry_in_seconds=%d",
                    trace_id,
                    phase_id,
                    role_dir,
                    sub_role,
                    call_kind,
                    attempt,
                    self._max_attempts,
                    str(exc),
                    sleep_seconds,
                )
                self._observe_retry_backoff(
                    trace_id=trace_id,
                    phase_id=phase_id,
                    role_dir=role_dir,
                    sub_role=sub_role,
                    call_kind=call_kind,
                    attempt=attempt,
                    sleep_seconds=sleep_seconds,
                )
        raise RuntimeError(
            f"Direct LLM provider {call_kind} failed after {self._max_attempts} attempts: {last_exc}"
        )

    # SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver._invoke_with_retry:v1

    # SEM_BEGIN orchestrator_v1.direct_llm_driver.direct_llm_driver.run_task:v1
    # type: METHOD
    # use_case: Выполняет один runtime step через single-shot ChatOpenAI вызов и парсит YAML контракт.
    # feature:
    #   - Step-level runtime может обойтись без OpenHands там, где нужны только reasoning + structured output
    # pre:
    #   - request.prompt is not empty
    #   - self._llm_api_key is configured
    # post:
    #   - returns a DriverResult with normalized status and parsed payload
    # invariant:
    #   - request object is not mutated
    # modifies (internal):
    #   -
    # emits (external):
    #   - external.llm_provider
    # errors:
    #   - RuntimeError: direct LLM call failed
    # depends:
    #   - ChatOpenAI
    # sft: execute one task unit step with a direct LLM call and parse a single YAML block response
    # idempotent: false
    # logs: query: DirectLlmDriver trace_id
    @observe(name="direct_llm_run_task")
    def run_task(self, request: DriverRequest) -> DriverResult:
        trace_id = ensure_trace_id(request.metadata.get("trace_id"))
        logger.info(
            "[DirectLlmDriver][run_task][ContextAnchor] trace_id=%s | "
            "Running direct LLM step. phase=%s, role_dir=%s, sub_role=%s, model=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            request.model,
        )

        if not request.prompt.strip():
            logger.warning(
                "[DirectLlmDriver][run_task][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
                "Empty prompt for direct LLM step. phase=%s, role_dir=%s, sub_role=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
            )
            raise RuntimeError("DirectLlmDriver received an empty prompt")
        if not self._llm_api_key:
            logger.warning(
                "[DirectLlmDriver][run_task][ErrorHandled][ERR:PRECONDITION] trace_id=%s | "
                "Missing direct LLM API key. phase=%s, role_dir=%s, sub_role=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
            )
            raise RuntimeError("DirectLlmDriver requires an API key")

        try:
            initial_messages: list[SystemMessage | HumanMessage | AIMessage] = []
            if request.system_prompt:
                initial_messages.append(SystemMessage(content=request.system_prompt))
                user_content = request.prompt[len(request.system_prompt):].lstrip("\n")
                initial_messages.append(HumanMessage(content=user_content or request.prompt))
            else:
                initial_messages.append(HumanMessage(content=request.prompt))
            hard_timeout_seconds, idle_timeout_seconds = self._resolve_timeout_settings(
                request=request,
                messages=initial_messages,
                trace_id=trace_id,
            )
            llm_factory = lambda: self._build_llm(
                model=request.model,
                timeout_seconds=hard_timeout_seconds,
            )
            response = self._invoke_with_retry(
                llm_factory=llm_factory,
                messages=initial_messages,
                hard_timeout_seconds=hard_timeout_seconds,
                idle_timeout_seconds=idle_timeout_seconds,
                trace_id=trace_id,
                phase_id=str(request.phase_id),
                role_dir=request.role_dir,
                sub_role=str(request.sub_role),
                call_kind="primary",
            )
            raw_text = extract_text_content(response.content)
            if not raw_text:
                status = status_for_parse_failure(request.sub_role)
                parsed_payload = {
                    "status": str(status),
                    "warnings": ["Direct LLM returned no parseable text output"],
                }
            else:
                parsed_payload = coerce_payload(raw_text)
                if parsed_payload is None:
                    status = status_for_parse_failure(request.sub_role)
                    parsed_payload = {
                        "status": str(status),
                        "warnings": [
                            "Direct LLM returned non-YAML final output; final reply must be exactly one YAML block",
                        ],
                    }
                else:
                    if "verdict" in parsed_payload and "status" not in parsed_payload:
                        parsed_payload["status"] = parsed_payload["verdict"]
                    parsed_payload = normalize_payload_shape(
                        request.phase_id,
                        request.sub_role,
                        parsed_payload,
                    )
                    status = normalize_status(str(parsed_payload.get("status") or "PASS"), request.sub_role)
                    missing_keys = missing_required_payload_keys(
                        request.phase_id,
                        request.sub_role,
                        parsed_payload,
                        request.task_context,
                    )
                    if missing_keys:
                        repair_prompt = (
                            "Your previous YAML response was missing required keys: "
                            + ", ".join(missing_keys)
                            + ". Rewrite the answer as exactly one YAML block with all required keys present. "
                            + "Do not add prose before or after the YAML."
                        )
                        repair_messages: list[SystemMessage | HumanMessage | AIMessage] = list(initial_messages) + [
                            AIMessage(content=raw_text),
                            HumanMessage(content=repair_prompt),
                        ]
                        repair_response = self._invoke_with_retry(
                            llm_factory=llm_factory,
                            messages=repair_messages,
                            hard_timeout_seconds=hard_timeout_seconds,
                            idle_timeout_seconds=idle_timeout_seconds,
                            trace_id=trace_id,
                            phase_id=str(request.phase_id),
                            role_dir=request.role_dir,
                            sub_role=str(request.sub_role),
                            call_kind="repair",
                        )
                        repaired_text = extract_text_content(repair_response.content)
                        repaired_payload = coerce_payload(repaired_text)
                        if repaired_payload is not None:
                            if "verdict" in repaired_payload and "status" not in repaired_payload:
                                repaired_payload["status"] = repaired_payload["verdict"]
                            repaired_payload = normalize_payload_shape(
                                request.phase_id,
                                request.sub_role,
                                repaired_payload,
                            )
                            repaired_missing = missing_required_payload_keys(
                                request.phase_id,
                                request.sub_role,
                                repaired_payload,
                                request.task_context,
                            )
                            if not repaired_missing:
                                raw_text = repaired_text
                                parsed_payload = repaired_payload
                                status = normalize_status(
                                    str(parsed_payload.get("status") or "PASS"),
                                    request.sub_role,
                                )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[DirectLlmDriver][run_task][ErrorHandled][ERR:EXTERNAL] trace_id=%s | "
                "Direct LLM execution failed. phase=%s, role_dir=%s, sub_role=%s, error=%s",
                trace_id,
                request.phase_id,
                request.role_dir,
                request.sub_role,
                str(exc),
            )
            raise RuntimeError(f"Direct LLM driver failed: {exc}") from exc

        logger.info(
            "[DirectLlmDriver][run_task][StepComplete] trace_id=%s | "
            "Direct LLM step completed. phase=%s, role_dir=%s, sub_role=%s, status=%s",
            trace_id,
            request.phase_id,
            request.role_dir,
            request.sub_role,
            status,
        )
        return DriverResult(
            status=status,
            payload=parsed_payload,
            raw_text=raw_text,
            conversation_id=None,
        )

    # SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver.run_task:v1


# SEM_END orchestrator_v1.direct_llm_driver.direct_llm_driver:v1
