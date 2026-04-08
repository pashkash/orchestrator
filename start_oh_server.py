#!/usr/bin/env python3
"""Wrapper to start OH agent-server with cross-process OTEL trace propagation.

Captures `x-lmnr-parent-ctx` on inbound HTTP requests, stores it per
conversation id, and patches OpenHands conversation spans so they can attach
to the orchestrator trace tree instead of becoming detached roots.
"""
import argparse
import json
import logging
import os
import sys
import traceback
from contextvars import ContextVar
from typing import Any
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger("openhands.agent_server.api.oh_trace_propagation")
logger.setLevel(logging.INFO)

_lmnr_parent_ctx: ContextVar[str | None] = ContextVar("_lmnr_parent_ctx", default=None)
_conversation_parent_ctx: dict[str, str] = {}
_conversation_span_link_status: dict[str, str] = {}
_conversation_root_spans: dict[str, Any] = {}
_logged_problematic_otel_request_stack = False
_EMPTY_CONVERSATION_TITLE = "Untitled conversation"
_MISSING_USER_MESSAGE_ERROR = "No user messages found in conversation events"
_LMNR_SELF_HOSTED_HTTP_PORT = 8000
_LAMINAR_ENV_ALIASES: tuple[tuple[str, str], ...] = (
    ("LAMINAR_BASE_URL", "LMNR_BASE_URL"),
    ("LAMINAR_PROJECT_API_KEY", "LMNR_PROJECT_API_KEY"),
    ("LAMINAR_HTTP_PORT", "LMNR_HTTP_PORT"),
    ("LAMINAR_GRPC_PORT", "LMNR_GRPC_PORT"),
)


def _is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _read_int_env(name: str) -> int | None:
    raw_value = str(os.getenv(name, "")).strip()
    if not raw_value:
        return None
    try:
        return int(raw_value)
    except ValueError:
        logger.warning("Ignoring invalid integer environment value %s=%r", name, raw_value)
        return None


def _build_http_otel_base_url(base_url: str, *, http_port: int | None) -> str | None:
    parsed = urlparse(base_url)
    scheme = str(parsed.scheme or "").strip().lower()
    hostname = str(parsed.hostname or "").strip()
    if not scheme or not hostname:
        return None
    resolved_port = parsed.port or http_port or (80 if scheme == "http" else 443)
    default_port = 80 if scheme == "http" else 443
    netloc = hostname if resolved_port == default_port else f"{hostname}:{resolved_port}"
    normalized_path = str(parsed.path or "").rstrip("/")
    return urlunparse((scheme, netloc, normalized_path, "", "", "")).rstrip("/")


def _rewrite_localhost_otel_endpoint_url(url: str | None) -> str | None:
    base_url = str(os.getenv("LMNR_BASE_URL", "")).strip()
    normalized_base = _build_http_otel_base_url(base_url, http_port=_read_int_env("LMNR_HTTP_PORT"))
    if not normalized_base:
        return url
    parsed = urlparse(str(url or ""))
    if (
        parsed.scheme == "https"
        and str(parsed.hostname or "").strip().lower() in {"localhost", "127.0.0.1"}
        and (parsed.port in {None, 443})
        and parsed.path in {"/v1/traces", "/v1/logs"}
    ):
        return f"{normalized_base}{parsed.path}"
    return url


def _normalize_laminar_environment(environ: dict[str, str] | None = None) -> dict[str, str]:
    target_env = os.environ if environ is None else environ
    normalized: dict[str, str] = {}
    for source_name, target_name in _LAMINAR_ENV_ALIASES:
        source_value = str(target_env.get(source_name, "")).strip()
        target_value = str(target_env.get(target_name, "")).strip()
        if source_value and not target_value:
            target_env[target_name] = source_value
            normalized[target_name] = f"alias:{source_name}"
        elif source_value and target_value and source_value != target_value:
            logger.warning(
                "Laminar env alias conflict detected: %s=%r ignored because %s=%r is already set",
                source_name,
                source_value,
                target_name,
                target_value,
            )

    base_url = str(target_env.get("LMNR_BASE_URL", "")).strip()
    if base_url:
        parsed = urlparse(base_url)
        has_explicit_port = parsed.port is not None
        if parsed.scheme == "http":
            if not str(target_env.get("LMNR_HTTP_PORT", "")).strip() and not has_explicit_port:
                target_env["LMNR_HTTP_PORT"] = str(_LMNR_SELF_HOSTED_HTTP_PORT)
                normalized["LMNR_HTTP_PORT"] = "derived:self-hosted-http-default"
            if not str(target_env.get("LMNR_FORCE_HTTP", "")).strip():
                target_env["LMNR_FORCE_HTTP"] = "1"
                normalized["LMNR_FORCE_HTTP"] = "derived:http-scheme"
            raw_http_port = str(target_env.get("LMNR_HTTP_PORT", "")).strip()
            http_port: int | None = None
            if raw_http_port:
                try:
                    http_port = int(raw_http_port)
                except ValueError:
                    logger.warning(
                        "Ignoring invalid integer environment value %s=%r",
                        "LMNR_HTTP_PORT",
                        raw_http_port,
                    )
            otel_base_url = _build_http_otel_base_url(base_url, http_port=http_port)
            if otel_base_url:
                derived_otel_env = {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": otel_base_url,
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": f"{otel_base_url}/v1/traces",
                    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": f"{otel_base_url}/v1/logs",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                    "OTEL_EXPORTER": "otlp_http",
                }
                for env_name, env_value in derived_otel_env.items():
                    if not str(target_env.get(env_name, "")).strip():
                        target_env[env_name] = env_value
                        normalized[env_name] = "derived:lmnr-http-bootstrap"
    return normalized


def _patch_lmnr_http_exporter_defaults() -> None:
    from lmnr.opentelemetry_lib.tracing import exporter as lmnr_exporter

    current_impl = getattr(lmnr_exporter, "_configure_exporter", None)
    if getattr(current_impl, "__oh_lmnr_http_exporter_defaults_patch__", False):
        return

    original_impl = current_impl

    def _patched_configure_exporter(
        base_url: str | None,
        port: int | None,
        api_key: str | None,
        timeout_seconds: int,
        force_http: bool,
    ) -> dict[str, Any]:
        config = original_impl(base_url, port, api_key, timeout_seconds, force_http)
        if not bool(config.get("force_http")):
            return config

        resolved_base_url = str(base_url or os.getenv("LMNR_BASE_URL", "")).strip()
        parsed_base_url = urlparse(resolved_base_url) if resolved_base_url else None
        if parsed_base_url is None or parsed_base_url.scheme != "http":
            return config

        resolved_port = port
        if resolved_port is None:
            raw_http_port = str(os.getenv("LMNR_HTTP_PORT", "")).strip()
            if raw_http_port:
                try:
                    resolved_port = int(raw_http_port)
                except ValueError:
                    logger.warning(
                        "Ignoring invalid integer environment value %s=%r",
                        "LMNR_HTTP_PORT",
                        raw_http_port,
                    )
        normalized_base = _build_http_otel_base_url(resolved_base_url, http_port=resolved_port)
        if not normalized_base:
            return config

        endpoint = str(config.get("endpoint") or "").strip()
        endpoint_path = ""
        if endpoint:
            parsed_endpoint = urlparse(endpoint)
            if parsed_endpoint.path and parsed_endpoint.path != "/":
                endpoint_path = parsed_endpoint.path
            elif "/v1/" in endpoint:
                endpoint_path = endpoint[endpoint.index("/v1/") :]
        if endpoint_path:
            config["endpoint"] = f"{normalized_base}{endpoint_path}"
        else:
            config["endpoint"] = normalized_base
        return config

    setattr(_patched_configure_exporter, "__oh_lmnr_http_exporter_defaults_patch__", True)
    lmnr_exporter._configure_exporter = _patched_configure_exporter
    logger.info("Patched lmnr HTTP exporter defaults for force_http self-hosted endpoints")


def _patch_otel_http_exporter_endpoints() -> None:
    from opentelemetry.exporter.otlp.proto.http._log_exporter import (
        OTLPLogExporter as HttpOtlpLogExporter,
    )
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as HttpOtlpSpanExporter,
    )

    def _resolve_local_http_endpoint(default_path: str) -> str | None:
        base_url = str(os.getenv("LMNR_BASE_URL", "")).strip()
        if not base_url or urlparse(base_url).scheme != "http":
            return None
        normalized_base = _build_http_otel_base_url(base_url, http_port=_read_int_env("LMNR_HTTP_PORT"))
        if not normalized_base:
            return None
        return f"{normalized_base}{default_path}"

    def _should_override_endpoint(endpoint: str | None) -> bool:
        if not str(endpoint or "").strip():
            return True
        parsed = urlparse(str(endpoint))
        hostname = str(parsed.hostname or "").strip().lower()
        if hostname not in {"localhost", "127.0.0.1"}:
            return False
        if parsed.scheme == "https":
            return True
        if parsed.scheme not in {"http", "https"}:
            return True
        if parsed.port == 443:
            return True
        return False

    def _patch_class(cls, *, default_path: str, marker: str) -> None:
        current_init = getattr(cls, "__init__", None)
        if getattr(current_init, marker, False):
            return
        original_init = current_init

        def _wrapped_init(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            endpoint = kwargs.get("endpoint")
            if endpoint is None and args:
                endpoint = args[0]
            if _should_override_endpoint(endpoint):
                resolved_endpoint = _resolve_local_http_endpoint(default_path)
                if resolved_endpoint:
                    kwargs["endpoint"] = resolved_endpoint
            return original_init(self, *args, **kwargs)

        setattr(_wrapped_init, marker, True)
        cls.__init__ = _wrapped_init

    _patch_class(
        HttpOtlpSpanExporter,
        default_path="/v1/traces",
        marker="__oh_otel_http_span_exporter_endpoint_patch__",
    )
    _patch_class(
        HttpOtlpLogExporter,
        default_path="/v1/logs",
        marker="__oh_otel_http_log_exporter_endpoint_patch__",
    )
    logger.info("Patched OpenTelemetry HTTP exporter endpoints for self-hosted Laminar")


def _patch_otel_http_exporter_runtime_endpoints() -> None:
    from opentelemetry.exporter.otlp.proto.http._log_exporter import (
        OTLPLogExporter as HttpOtlpLogExporter,
    )
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as HttpOtlpSpanExporter,
    )

    def _patch_class(cls, *, marker: str) -> None:
        current_export = getattr(cls, "_export", None)
        if getattr(current_export, marker, False):
            return
        original_export = current_export

        def _wrapped_export(self, serialized_data, timeout_sec=None):  # noqa: ANN001, ANN201
            current_endpoint = getattr(self, "_endpoint", None)
            rewritten_endpoint = _rewrite_localhost_otel_endpoint_url(current_endpoint)
            if rewritten_endpoint and rewritten_endpoint != current_endpoint:
                logger.info(
                    "Rewriting OTEL exporter runtime endpoint from %s to %s",
                    current_endpoint,
                    rewritten_endpoint,
                )
                self._endpoint = rewritten_endpoint
            return original_export(self, serialized_data, timeout_sec)

        setattr(_wrapped_export, marker, True)
        cls._export = _wrapped_export

    _patch_class(
        HttpOtlpSpanExporter,
        marker="__oh_otel_http_span_export_runtime_patch__",
    )
    _patch_class(
        HttpOtlpLogExporter,
        marker="__oh_otel_http_log_export_runtime_patch__",
    )
    logger.info("Patched OpenTelemetry HTTP exporter runtime endpoints")


def _patch_requests_localhost_otel_urls() -> None:
    import requests

    current_request = getattr(requests.sessions.Session, "request", None)
    if getattr(current_request, "__oh_requests_localhost_otel_url_patch__", False):
        return

    original_request = current_request

    def _wrapped_request(self, method, url, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        rewritten_url = _rewrite_localhost_otel_endpoint_url(str(url or ""))
        if rewritten_url != url:
            logger.info(
                "Rewriting OTEL exporter request URL from %s to %s",
                url,
                rewritten_url,
            )
        return original_request(self, method, rewritten_url, *args, **kwargs)

    setattr(_wrapped_request, "__oh_requests_localhost_otel_url_patch__", True)
    requests.sessions.Session.request = _wrapped_request
    logger.info("Patched requests Session.request for localhost OTEL exporter URLs")


def _patch_requests_http_adapter_send_debug() -> None:
    import requests

    current_send = getattr(requests.adapters.HTTPAdapter, "send", None)
    if getattr(current_send, "__oh_requests_http_adapter_send_debug_patch__", False):
        return

    original_send = current_send

    def _wrapped_send(self, request, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        global _logged_problematic_otel_request_stack

        request_url = str(getattr(request, "url", "") or "")
        parsed = urlparse(request_url)
        if (
            not _logged_problematic_otel_request_stack
            and parsed.scheme == "https"
            and str(parsed.hostname or "").strip().lower() in {"localhost", "127.0.0.1"}
            and (parsed.port in {None, 443})
            and parsed.path in {"/v1/traces", "/v1/logs"}
        ):
            _logged_problematic_otel_request_stack = True
            logger.warning(
                "Observed raw HTTPS localhost OTEL request before transport. url=%s\n%s",
                request_url,
                "".join(traceback.format_stack(limit=20)),
            )
        return original_send(self, request, *args, **kwargs)

    setattr(_wrapped_send, "__oh_requests_http_adapter_send_debug_patch__", True)
    requests.adapters.HTTPAdapter.send = _wrapped_send
    logger.info("Patched requests HTTPAdapter.send debug hook for OTEL URLs")


def _initialize_laminar_runtime() -> None:
    normalized = _normalize_laminar_environment()
    has_observability_env = any(
        str(os.getenv(name, "")).strip()
        for name in (
            "LMNR_PROJECT_API_KEY",
            "OTEL_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
        )
    )
    if not has_observability_env:
        logger.info("Laminar bootstrap skipped because no observability environment variables are set")
        return

    from lmnr import Laminar

    _patch_requests_http_adapter_send_debug()
    _patch_requests_localhost_otel_urls()
    _patch_otel_http_exporter_runtime_endpoints()
    _patch_otel_http_exporter_endpoints()
    _patch_lmnr_http_exporter_defaults()

    base_url = str(os.getenv("LMNR_BASE_URL", "")).strip() or None
    http_port = _read_int_env("LMNR_HTTP_PORT")
    grpc_port = _read_int_env("LMNR_GRPC_PORT")
    force_http = _is_truthy_env(os.getenv("LMNR_FORCE_HTTP"))
    logger.info(
        "Initializing Laminar runtime bootstrap. base_url=%s, http_port=%s, grpc_port=%s, "
        "force_http=%s, project_api_key=%s, normalized_env=%s",
        base_url or "<default>",
        http_port if http_port is not None else "<default>",
        grpc_port if grpc_port is not None else "<default>",
        force_http,
        "set" if str(os.getenv("LMNR_PROJECT_API_KEY", "")).strip() else "unset",
        ",".join(sorted(normalized)) if normalized else "<none>",
    )
    Laminar.initialize(
        project_api_key=str(os.getenv("LMNR_PROJECT_API_KEY", "")).strip() or None,
        base_url=base_url,
        http_port=http_port,
        grpc_port=grpc_port,
        force_http=force_http,
    )


# SEM_BEGIN orchestrator_v1.start_oh_server.lmnr_context_capture_middleware:v1
# type: CLASS
# brief: ASGI middleware that captures incoming Laminar parent context headers and binds them to OH conversation ids.
# pre:
# - app is an ASGI-compatible callable
# post:
# - requests with x-lmnr-parent-ctx update ContextVar and conversation-id map before downstream OH code runs
# invariant:
# - non-HTTP scopes pass through unchanged
# - response body bytes are preserved exactly even when buffered for conversation-id extraction
# modifies:
# - external.openhands_runtime
# errors:
# - -
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: FastAPI decorator middleware path did not produce live header-capture evidence; wrapping the ASGI app object is more reliable.
# depends:
# - ASGI app callable
# notes: Buffering is only applied for requests that already carry x-lmnr-parent-ctx.
class _LaminarContextCaptureMiddleware:
    __oh_lmnr_ctx_middleware__ = True

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope, receive, send) -> None:  # noqa: ANN001
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        ctx_header = headers.get("x-lmnr-parent-ctx", "")
        if not ctx_header:
            await self._app(scope, receive, send)
            return

        path = str(scope.get("path") or "")
        logger.info("MW: got x-lmnr-parent-ctx len=%d path=%s", len(ctx_header), path)
        token = _lmnr_parent_ctx.set(ctx_header)
        response_start: dict[str, Any] | None = None
        response_body = bytearray()
        stored_conversation_id = ""

        async def send_wrapper(message) -> None:  # noqa: ANN001
            nonlocal response_start, stored_conversation_id
            message_type = str(message.get("type") or "")
            if message_type == "http.response.start":
                response_start = dict(message)
                return
            if message_type != "http.response.body":
                await send(message)
                return

            response_body.extend(message.get("body", b""))
            if message.get("more_body", False):
                return

            parts = path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] == "api" and parts[1] == "conversations":
                if len(parts) == 2:
                    try:
                        payload = json.loads(bytes(response_body) or b"{}")
                        conv_id = str(payload.get("conversation_id") or payload.get("id") or "").strip()
                        if conv_id:
                            _conversation_parent_ctx[conv_id] = ctx_header
                            stored_conversation_id = conv_id
                            logger.info("MW: stored parent_ctx for new conversation %s", conv_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("MW: failed to extract conversation_id from response: %s", exc)
                elif len(parts) >= 3:
                    conv_id = str(parts[2] or "").strip()
                    if conv_id:
                        _conversation_parent_ctx[conv_id] = ctx_header
                        stored_conversation_id = conv_id
                        logger.info("MW: stored parent_ctx for conversation %s", conv_id)

            if response_start is not None:
                response_headers = list(response_start.get("headers", []))
                response_headers.append((b"x-oh-lmnr-parent-ctx-captured", b"1"))
                if stored_conversation_id:
                    response_headers.append(
                        (
                            b"x-oh-lmnr-conversation-id-stored",
                            stored_conversation_id.encode("utf-8", errors="ignore"),
                        )
                    )
                    response_headers.append(
                        (
                            b"x-oh-lmnr-span-link-status",
                            str(_conversation_span_link_status.get(stored_conversation_id, "-"))
                            .encode("utf-8", errors="ignore"),
                        )
                    )
                response_start["headers"] = response_headers
                await send(response_start)
            await send(
                {
                    "type": "http.response.body",
                    "body": bytes(response_body),
                    "more_body": False,
                }
            )

        try:
            await self._app(scope, receive, send_wrapper)
        finally:
            _lmnr_parent_ctx.reset(token)


# SEM_END orchestrator_v1.start_oh_server.lmnr_context_capture_middleware:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.disable_local_filestore_tool_spans:v1
# type: METHOD
# brief: Убирает низкоценные Laminar spans от LocalFileStore в OpenHands persistence.
# pre:
# - local_file_store_cls is None or exposes write/list/delete attributes
# post:
# - observed wrappers for write/list/delete are removed when present
# invariant:
# - business logic of LocalFileStore methods is preserved
# modifies:
# - external.openhands_runtime
# errors:
# - -
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: Эти spans засоряют Laminar и маскируют реальные pipeline/OH шаги.
# depends:
# - LocalFileStore
# notes: Снимаем только observability wrapper, не меняя саму файловую семантику.
def _disable_local_filestore_tool_spans(local_file_store_cls=None) -> None:
    """Strip low-value LocalFileStore tool spans from OpenHands persistence."""
    if local_file_store_cls is None:
        from openhands.sdk.io.local import LocalFileStore

        local_file_store_cls = LocalFileStore

    patched_methods: list[str] = []
    for method_name in ("write", "list", "delete"):
        method = getattr(local_file_store_cls, method_name, None)
        original = getattr(method, "__wrapped__", None)
        if callable(original):
            setattr(local_file_store_cls, method_name, original)
            patched_methods.append(method_name)

    logger.info(
        "Disabled LocalFileStore Laminar spans for methods: %s",
        ", ".join(patched_methods) if patched_methods else "<none>",
    )


# SEM_END orchestrator_v1.start_oh_server.disable_local_filestore_tool_spans:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.patch_empty_title_generation_fallback:v1
# type: METHOD
# brief: Подменяет генерацию title так, чтобы пустой conversation не создавал error-span.
# pre:
# - local_conversation_module exposes generate_conversation_title symbol or can be imported
# - title_utils_module exposes generate_conversation_title symbol or can be imported
# post:
# - missing-user-message title edge case returns fallback title instead of raising
# invariant:
# - all other title generation errors are still raised
# modifies:
# - external.openhands_runtime
# errors:
# - ValueError: re-raised for non-target title generation failures
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: Ложный ValueError красит Laminar trace в red, хотя pipeline фактически успешен.
# depends:
# - openhands.sdk.conversation.impl.local_conversation
# - openhands.sdk.conversation.title_utils
# notes: Патчим module-level function alias, чтобы observed method generate_title не эмитил error-span.
def _patch_empty_title_generation_fallback(
    local_conversation_module=None,
    title_utils_module=None,
) -> None:
    if local_conversation_module is None:
        import openhands.sdk.conversation.impl.local_conversation as local_conversation_module
    if title_utils_module is None:
        import openhands.sdk.conversation.title_utils as title_utils_module

    current_impl = getattr(local_conversation_module, "generate_conversation_title", None)
    if getattr(current_impl, "__oh_safe_title_patch__", False):
        logger.info("OpenHands conversation title fallback patch already applied")
        return

    original_impl = current_impl or getattr(title_utils_module, "generate_conversation_title")

    def _safe_generate_conversation_title(*args, **kwargs):  # noqa: ANN002, ANN003
        try:
            return original_impl(*args, **kwargs)
        except ValueError as exc:
            if _MISSING_USER_MESSAGE_ERROR not in str(exc):
                raise
            logger.info(
                "Returning fallback OpenHands conversation title because no user message was found"
            )
            return _EMPTY_CONVERSATION_TITLE

    _safe_generate_conversation_title.__oh_safe_title_patch__ = True
    local_conversation_module.generate_conversation_title = _safe_generate_conversation_title
    title_utils_module.generate_conversation_title = _safe_generate_conversation_title
    logger.info("Patched OpenHands conversation title generation fallback")


# SEM_END orchestrator_v1.start_oh_server.patch_empty_title_generation_fallback:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.patch_event_service_generate_title_fallback:v1
# type: METHOD
# brief: Подменяет async EventService.generate_title, чтобы auto-title path не логировал ложный warning.
# pre:
# - event_service_cls is None or exposes async generate_title method
# post:
# - missing-user-message edge case returns fallback title without raising
# invariant:
# - all non-target failures are still raised unchanged
# modifies:
# - external.openhands_runtime
# errors:
# - ValueError: re-raised for non-target title-generation failures
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: Нужно закрыть warning-path выше по стеку, а не надеяться только на lower-level title helper imports.
# depends:
# - openhands.agent_server.event_service.EventService
# notes: Это второй уровень защиты поверх patch на title_utils/local_conversation.
def _patch_event_service_generate_title_fallback(event_service_cls=None) -> None:
    if event_service_cls is None:
        from openhands.agent_server.event_service import EventService

        event_service_cls = EventService

    current_impl = getattr(event_service_cls, "generate_title", None)
    if getattr(current_impl, "__oh_safe_event_title_patch__", False):
        logger.info("OpenHands EventService title fallback patch already applied")
        return

    original_impl = current_impl

    async def _safe_generate_title(self, llm=None, max_length: int = 50):  # noqa: ANN001
        try:
            return await original_impl(self, llm=llm, max_length=max_length)
        except ValueError as exc:
            if _MISSING_USER_MESSAGE_ERROR not in str(exc):
                raise
            logger.info(
                "Returning fallback OpenHands event-service title because no user message was found"
            )
            return _EMPTY_CONVERSATION_TITLE

    _safe_generate_title.__oh_safe_event_title_patch__ = True
    event_service_cls.generate_title = _safe_generate_title
    logger.info("Patched OpenHands EventService.generate_title fallback")


# SEM_END orchestrator_v1.start_oh_server.patch_event_service_generate_title_fallback:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.patch_event_service_start_context_bridge:v1
# type: METHOD
# brief: Ставит parent Laminar context в conversation-id map до EventService.start, чтобы initial LocalConversation span мог связаться с orchestrator trace.
# pre:
# - event_service_cls is None or exposes async start method and stored.id
# post:
# - when x-lmnr-parent-ctx exists in current request scope, conversation id is seeded into _conversation_parent_ctx before LocalConversation initialization
# invariant:
# - original EventService.start semantics are preserved
# modifies:
# - external.openhands_runtime
# errors:
# - propagates original EventService.start exceptions unchanged
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: Middleware response-stage capture is too late for the very first native OH conversation span, which is created during EventService.start before the HTTP response is sent.
# depends:
# - openhands.agent_server.event_service.EventService
# - _lmnr_parent_ctx
# - _conversation_parent_ctx
# notes: This is a bridge for the initial conversation span only; later HTTP requests still use middleware capture confirmation.
def _patch_event_service_start_context_bridge(event_service_cls=None) -> None:
    if event_service_cls is None:
        from openhands.agent_server.event_service import EventService

        event_service_cls = EventService

    current_impl = getattr(event_service_cls, "start", None)
    if getattr(current_impl, "__oh_lmnr_start_bridge_patch__", False):
        logger.info("OpenHands EventService start bridge patch already applied")
        return

    original_impl = current_impl

    async def _start_with_parent_ctx_bridge(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        parent_ctx = _lmnr_parent_ctx.get()
        conversation_id = str(getattr(getattr(self, "stored", None), "id", "") or "").strip()
        if parent_ctx and conversation_id:
            _conversation_parent_ctx[conversation_id] = parent_ctx
            logger.info(
                "Seeded parent_ctx before EventService.start for conversation %s",
                conversation_id,
            )
        return await original_impl(self, *args, **kwargs)

    _start_with_parent_ctx_bridge.__oh_lmnr_start_bridge_patch__ = True
    event_service_cls.start = _start_with_parent_ctx_bridge
    logger.info("Patched OpenHands EventService.start context bridge")


# SEM_END orchestrator_v1.start_oh_server.patch_event_service_start_context_bridge:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.patch_local_conversation_runtime_span_bridge:v1
# type: METHOD
# brief: Реактивирует conversation root span внутри LocalConversation.run/send_message, чтобы child OH spans не терялись на границе request/background-thread.
# pre:
# - local_conversation_cls is None or exposes run/send_message methods and _state.id
# post:
# - when a linked conversation root span exists, LocalConversation.run/send_message execute under Laminar.use_span(span)
# invariant:
# - original LocalConversation.run/send_message business behavior is preserved
# modifies:
# - external.openhands_runtime
# errors:
# - propagates original LocalConversation exceptions unchanged
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: conversation span starts during /api/conversations, but real OH work happens later in /run executor threads; without reactivation, native child spans lose the conversation parent.
# depends:
# - openhands.sdk.conversation.impl.local_conversation.LocalConversation
# - _conversation_root_spans
# notes: Wrapper activates the existing root span only for the duration of the concrete sync method call.
def _patch_local_conversation_runtime_span_bridge(local_conversation_cls=None) -> None:
    if local_conversation_cls is None:
        from openhands.sdk.conversation.impl.local_conversation import LocalConversation

        local_conversation_cls = LocalConversation

    def _wrap_method(method_name: str, patch_marker: str) -> None:
        current_impl = getattr(local_conversation_cls, method_name, None)
        if getattr(current_impl, patch_marker, False):
            return

        original_impl = current_impl

        def _wrapped(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            session_id = str(getattr(getattr(self, "_state", None), "id", "") or "").strip()
            root_span = _conversation_root_spans.get(session_id)
            if root_span is None:
                return original_impl(self, *args, **kwargs)

            from lmnr import Laminar

            logger.info(
                "Reactivating conversation root span for LocalConversation.%s session=%s",
                method_name,
                session_id,
            )
            with Laminar.use_span(root_span):
                return original_impl(self, *args, **kwargs)

        setattr(_wrapped, patch_marker, True)
        setattr(local_conversation_cls, method_name, _wrapped)

    _wrap_method("run", "__oh_lmnr_run_bridge_patch__")
    _wrap_method("send_message", "__oh_lmnr_send_bridge_patch__")
    logger.info("Patched LocalConversation runtime span bridge")


# SEM_END orchestrator_v1.start_oh_server.patch_local_conversation_runtime_span_bridge:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.patch_event_service_runtime_span_bridge:v1
# type: METHOD
# brief: Реактивирует conversation root span вокруг EventService.run/send_message, чтобы Laminar context переживал asyncio task creation.
# pre:
# - event_service_cls is None or exposes async run/send_message methods plus stored.id or _conversation._state.id
# post:
# - when a linked conversation root span exists, EventService.run/send_message execute under Laminar.use_span(span)
# invariant:
# - original EventService async semantics are preserved
# modifies:
# - external.openhands_runtime
# errors:
# - propagates original EventService exceptions unchanged
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: OpenHands starts background asyncio tasks before hopping into LocalConversation executor threads; those tasks must inherit the linked Laminar parent context.
# depends:
# - openhands.agent_server.event_service.EventService
# - _conversation_root_spans
# notes: Complements LocalConversation wrappers by restoring the parent span one async boundary earlier.
def _patch_event_service_runtime_span_bridge(event_service_cls=None) -> None:
    if event_service_cls is None:
        from openhands.agent_server.event_service import EventService

        event_service_cls = EventService

    def _wrap_method(method_name: str, patch_marker: str) -> None:
        current_impl = getattr(event_service_cls, method_name, None)
        if getattr(current_impl, patch_marker, False):
            return

        original_impl = current_impl

        async def _wrapped(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            conversation = getattr(self, "_conversation", None)
            session_id = str(
                getattr(getattr(conversation, "_state", None), "id", "")
                or getattr(getattr(self, "stored", None), "id", "")
                or ""
            ).strip()
            root_span = _conversation_root_spans.get(session_id)
            if root_span is None:
                return await original_impl(self, *args, **kwargs)

            from lmnr import Laminar

            logger.info(
                "Reactivating conversation root span for EventService.%s session=%s",
                method_name,
                session_id,
            )
            with Laminar.use_span(root_span):
                return await original_impl(self, *args, **kwargs)

        setattr(_wrapped, patch_marker, True)
        setattr(event_service_cls, method_name, _wrapped)

    _wrap_method("run", "__oh_lmnr_event_service_run_bridge_patch__")
    _wrap_method("send_message", "__oh_lmnr_event_service_send_bridge_patch__")
    logger.info("Patched OpenHands EventService runtime span bridge")


# SEM_END orchestrator_v1.start_oh_server.patch_event_service_runtime_span_bridge:v1


# SEM_BEGIN orchestrator_v1.start_oh_server.patch_all:v1
# type: METHOD
# brief: Применяет все runtime monkey-patches для корректной трассировки и чистых Laminar traces.
# pre:
# - OpenHands SDK modules are importable in the current runtime
# post:
# - OH conversation spans link into orchestrator trace tree
# - LocalFileStore noise spans are disabled
# - empty title generation no longer produces false error spans
# invariant:
# - OpenHands API app remains callable after patching
# modifies:
# - external.openhands_runtime
# errors:
# - -
# feature:
# - docs/project_specific/AGENTS_PROJECT.md
# - orchestrator/README.md
# why: Все runtime patch points должны жить в одном server wrapper, а не быть размазаны по orchestrator-коду.
# depends:
# - BaseConversation
# - openhands.agent_server.api
# notes: Middleware и monkey-patches применяются до старта uvicorn.
def _patch_all():
    from openhands.sdk.observability.laminar import (
        should_enable_observability,
        _get_span_manager,
    )
    import openhands.sdk.conversation.base as conv_base

    # OpenHands persists conversation state through LocalFileStore. Those spans are
    # diagnostic noise for our pipeline view and sometimes appear as stray roots.
    _disable_local_filestore_tool_spans()
    _patch_empty_title_generation_fallback()
    _patch_event_service_generate_title_fallback()
    _patch_event_service_start_context_bridge()
    _patch_event_service_runtime_span_bridge()
    _patch_local_conversation_runtime_span_bridge()

    _orig_start_span = conv_base.BaseConversation._start_observability_span
    _orig_end_span = conv_base.BaseConversation._end_observability_span

    def _patched_start_span(self, session_id: str) -> None:
        if not should_enable_observability():
            return
        resolved_session_id = str(session_id).strip()
        parent_ctx_str = _lmnr_parent_ctx.get() or _conversation_parent_ctx.pop(resolved_session_id, None)
        logger.info(
            "_start_observability_span: session=%s parent_ctx=%s",
            resolved_session_id, "present" if parent_ctx_str else "absent",
        )
        _conversation_span_link_status[resolved_session_id] = "present" if parent_ctx_str else "absent"
        if parent_ctx_str:
            try:
                from lmnr import Laminar
                from lmnr.sdk.laminar import LaminarSpanContext
                parent_ctx = LaminarSpanContext.deserialize(parent_ctx_str)
                span = Laminar.start_active_span(
                    "conversation",
                    session_id=resolved_session_id,
                    parent_span_context=parent_ctx,
                )
                _conversation_root_spans[resolved_session_id] = span
                _get_span_manager()._stack.append(span)
                _conversation_span_link_status[resolved_session_id] = "linked"
                logger.info("conversation span linked to orchestrator and stored for runtime reactivation")
                return
            except Exception as e:
                _conversation_span_link_status[resolved_session_id] = "link_error"
                logger.error("Link failed: %s", e, exc_info=True)
        _orig_start_span(self, session_id)

    def _patched_end_span(self) -> None:
        resolved_session_id = str(getattr(getattr(self, "_state", None), "id", "") or "").strip()
        try:
            _orig_end_span(self)
        finally:
            if resolved_session_id:
                _conversation_root_spans.pop(resolved_session_id, None)

    conv_base.BaseConversation._start_observability_span = _patched_start_span
    conv_base.BaseConversation._end_observability_span = _patched_end_span

    import openhands.agent_server.api as api_module

    if not getattr(api_module.api, "__oh_lmnr_ctx_middleware__", False):
        api_module.api = _LaminarContextCaptureMiddleware(api_module.api)
        logger.info("Wrapped OpenHands ASGI app with Laminar context capture middleware")


# SEM_END orchestrator_v1.start_oh_server.patch_all:v1


def _run_server() -> int:
    from uvicorn import Config

    from openhands.agent_server.__main__ import LoggingServer
    from openhands.agent_server.api import api
    from openhands.agent_server.logging_config import LOGGING_CONFIG
    from openhands.sdk.logger import DEBUG

    parser = argparse.ArgumentParser(description="OpenHands Agent Server App")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--reload",
        dest="reload",
        default=False,
        action="store_true",
    )
    args = parser.parse_args()

    log_level = "debug" if DEBUG else "info"
    config = Config(
        api,
        host=args.host,
        port=args.port,
        reload=args.reload,
        reload_includes=[
            "openhands-agent-server",
            "openhands-sdk",
            "openhands-tools",
        ],
        log_level=log_level,
        log_config=LOGGING_CONFIG,
        ws="wsproto",
    )
    server = LoggingServer(config)
    server.run()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _initialize_laminar_runtime()
    _patch_all()
    sys.exit(_run_server())
