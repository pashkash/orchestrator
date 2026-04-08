from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from start_oh_server import (
    _LaminarContextCaptureMiddleware,
    _conversation_parent_ctx,
    _conversation_root_spans,
    _conversation_span_link_status,
    _disable_local_filestore_tool_spans,
    _initialize_laminar_runtime,
    _patch_empty_title_generation_fallback,
    _patch_event_service_generate_title_fallback,
    _patch_event_service_runtime_span_bridge,
    _patch_event_service_start_context_bridge,
    _patch_lmnr_http_exporter_defaults,
    _patch_local_conversation_runtime_span_bridge,
    _lmnr_parent_ctx,
    _normalize_laminar_environment,
    _rewrite_localhost_otel_endpoint_url,
)


def _wrap(fn):
    def wrapped(*args, **kwargs):  # noqa: ANN002, ANN003
        return fn(*args, **kwargs)

    wrapped.__wrapped__ = fn
    return wrapped


class _FakeLocalFileStore:
    @_wrap
    def write(self, path, contents):  # noqa: ANN001, ANN201
        return ("write", path, contents)

    @_wrap
    def list(self, path):  # noqa: ANN001, ANN201
        return ["before", path]

    @_wrap
    def delete(self, path):  # noqa: ANN001, ANN201
        return ("delete", path)


def test_disable_local_filestore_tool_spans_unwraps_observed_methods() -> None:
    _disable_local_filestore_tool_spans(_FakeLocalFileStore)

    store = _FakeLocalFileStore()
    assert not hasattr(_FakeLocalFileStore.write, "__wrapped__")
    assert not hasattr(_FakeLocalFileStore.list, "__wrapped__")
    assert not hasattr(_FakeLocalFileStore.delete, "__wrapped__")
    assert store.write("a.txt", "x") == ("write", "a.txt", "x")
    assert store.list("dir") == ["before", "dir"]
    assert store.delete("a.txt") == ("delete", "a.txt")


def test_patch_empty_title_generation_fallback_returns_default_title() -> None:
    def _raise_missing_user(*args, **kwargs):  # noqa: ANN002, ANN003
        raise ValueError("No user messages found in conversation events")

    local_conversation_module = SimpleNamespace(generate_conversation_title=_raise_missing_user)
    title_utils_module = SimpleNamespace(generate_conversation_title=_raise_missing_user)

    _patch_empty_title_generation_fallback(
        local_conversation_module=local_conversation_module,
        title_utils_module=title_utils_module,
    )

    assert (
        local_conversation_module.generate_conversation_title(events=[], llm=None, max_length=50)
        == "Untitled conversation"
    )
    assert (
        title_utils_module.generate_conversation_title(events=[], llm=None, max_length=50)
        == "Untitled conversation"
    )


def test_patch_empty_title_generation_fallback_keeps_other_errors() -> None:
    def _raise_other(*args, **kwargs):  # noqa: ANN002, ANN003
        raise ValueError("unexpected title failure")

    local_conversation_module = SimpleNamespace(generate_conversation_title=_raise_other)
    title_utils_module = SimpleNamespace(generate_conversation_title=_raise_other)

    _patch_empty_title_generation_fallback(
        local_conversation_module=local_conversation_module,
        title_utils_module=title_utils_module,
    )

    with pytest.raises(ValueError, match="unexpected title failure"):
        local_conversation_module.generate_conversation_title(events=[], llm=None, max_length=50)


def test_patch_event_service_generate_title_fallback_returns_default_title() -> None:
    class _FakeEventService:
        async def generate_title(self, llm=None, max_length: int = 50):  # noqa: ANN001
            raise ValueError("No user messages found in conversation events")

    _patch_event_service_generate_title_fallback(_FakeEventService)

    assert asyncio.run(_FakeEventService().generate_title()) == "Untitled conversation"


def test_patch_event_service_generate_title_fallback_keeps_other_errors() -> None:
    class _FakeEventService:
        async def generate_title(self, llm=None, max_length: int = 50):  # noqa: ANN001
            raise ValueError("unexpected title failure")

    _patch_event_service_generate_title_fallback(_FakeEventService)

    with pytest.raises(ValueError, match="unexpected title failure"):
        asyncio.run(_FakeEventService().generate_title())


def test_laminar_context_capture_middleware_stores_parent_ctx_for_new_conversation() -> None:
    _conversation_parent_ctx.clear()
    _conversation_span_link_status.clear()
    _conversation_span_link_status["conv-123"] = "linked"

    async def fake_app(scope, receive, send):  # noqa: ANN001
        del scope, receive
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps({"conversation_id": "conv-123"}).encode(),
                "more_body": False,
            }
        )

    wrapper = _LaminarContextCaptureMiddleware(fake_app)
    sent_messages: list[dict] = []

    async def fake_receive():  # noqa: ANN201
        return {"type": "http.request", "body": b"", "more_body": False}

    async def fake_send(message):  # noqa: ANN001
        sent_messages.append(message)

    asyncio.run(
        wrapper(
            {
                "type": "http",
                "path": "/api/conversations",
                "headers": [(b"x-lmnr-parent-ctx", b"ctx-123")],
            },
            fake_receive,
            fake_send,
        )
    )

    assert _conversation_parent_ctx["conv-123"] == "ctx-123"
    assert sent_messages[0]["type"] == "http.response.start"
    response_headers = dict(sent_messages[0]["headers"])
    assert response_headers[b"x-oh-lmnr-span-link-status"] == b"linked"
    assert sent_messages[1]["body"] == b'{"conversation_id": "conv-123"}'


def test_patch_event_service_start_context_bridge_seeds_parent_ctx_before_start() -> None:
    _conversation_parent_ctx.clear()

    class _FakeEventService:
        def __init__(self) -> None:
            self.stored = SimpleNamespace(id="conv-start-123")
            self.seen_parent_ctx = ""

        async def start(self):  # noqa: ANN201
            self.seen_parent_ctx = _conversation_parent_ctx.get("conv-start-123", "")
            return "started"

    _patch_event_service_start_context_bridge(_FakeEventService)
    token = _lmnr_parent_ctx.set("ctx-start-123")
    try:
        service = _FakeEventService()
        result = asyncio.run(service.start())
    finally:
        _lmnr_parent_ctx.reset(token)

    assert result == "started"
    assert service.seen_parent_ctx == "ctx-start-123"


def test_patch_local_conversation_runtime_span_bridge_uses_root_span(monkeypatch) -> None:
    _conversation_root_spans.clear()

    class _FakeLaminar:
        used_spans: list[object] = []

        @classmethod
        def use_span(cls, span):  # noqa: ANN001
            class _Ctx:
                def __enter__(self_inner):  # noqa: ANN001
                    cls.used_spans.append(span)
                    return span

                def __exit__(self_inner, exc_type, exc, tb):  # noqa: ANN001
                    return False

            return _Ctx()

    class _FakeConversation:
        def __init__(self) -> None:
            self._state = SimpleNamespace(id="conv-run-123")
            self.calls: list[tuple[str, object | None]] = []

        def run(self):  # noqa: ANN201
            self.calls.append(("run", None))
            return "ran"

        def send_message(self, message, sender=None):  # noqa: ANN001, ANN201
            self.calls.append(("send_message", message))
            return sender

    monkeypatch.setitem(sys.modules, "lmnr", SimpleNamespace(Laminar=_FakeLaminar))
    _conversation_root_spans["conv-run-123"] = object()
    _patch_local_conversation_runtime_span_bridge(_FakeConversation)

    conversation = _FakeConversation()
    assert conversation.run() == "ran"
    assert conversation.send_message("hello") is None
    assert conversation.calls == [("run", None), ("send_message", "hello")]
    assert _FakeLaminar.used_spans == [
        _conversation_root_spans["conv-run-123"],
        _conversation_root_spans["conv-run-123"],
    ]


def test_patch_local_conversation_runtime_span_bridge_noops_without_root_span(monkeypatch) -> None:
    _conversation_root_spans.clear()

    class _FakeLaminar:
        used_spans: list[object] = []

        @classmethod
        def use_span(cls, span):  # noqa: ANN001
            cls.used_spans.append(span)
            raise AssertionError("use_span should not be called without a stored root span")

    class _FakeConversation:
        def __init__(self) -> None:
            self._state = SimpleNamespace(id="conv-run-missing")
            self.calls = 0

        def run(self):  # noqa: ANN201
            self.calls += 1
            return "ran"

    monkeypatch.setitem(sys.modules, "lmnr", SimpleNamespace(Laminar=_FakeLaminar))
    _patch_local_conversation_runtime_span_bridge(_FakeConversation)

    conversation = _FakeConversation()
    assert conversation.run() == "ran"
    assert conversation.calls == 1


def test_patch_event_service_runtime_span_bridge_uses_root_span(monkeypatch) -> None:
    _conversation_root_spans.clear()

    class _FakeLaminar:
        used_spans: list[object] = []

        @classmethod
        def use_span(cls, span):  # noqa: ANN001
            class _Ctx:
                def __enter__(self_inner):  # noqa: ANN001
                    cls.used_spans.append(span)
                    return span

                def __exit__(self_inner, exc_type, exc, tb):  # noqa: ANN001
                    return False

            return _Ctx()

    class _FakeEventService:
        def __init__(self) -> None:
            self._conversation = SimpleNamespace(_state=SimpleNamespace(id="conv-service-123"))
            self.calls: list[tuple[str, object | None]] = []

        async def run(self):  # noqa: ANN201
            self.calls.append(("run", None))
            return "running"

        async def send_message(self, message, run=False):  # noqa: ANN001, ANN201
            self.calls.append(("send_message", message))
            return run

    monkeypatch.setitem(sys.modules, "lmnr", SimpleNamespace(Laminar=_FakeLaminar))
    _conversation_root_spans["conv-service-123"] = object()
    _patch_event_service_runtime_span_bridge(_FakeEventService)

    service = _FakeEventService()
    assert asyncio.run(service.run()) == "running"
    assert asyncio.run(service.send_message("hello", run=True)) is True
    assert service.calls == [("run", None), ("send_message", "hello")]
    assert _FakeLaminar.used_spans == [
        _conversation_root_spans["conv-service-123"],
        _conversation_root_spans["conv-service-123"],
    ]


def test_patch_event_service_runtime_span_bridge_noops_without_root_span(monkeypatch) -> None:
    _conversation_root_spans.clear()

    class _FakeLaminar:
        used_spans: list[object] = []

        @classmethod
        def use_span(cls, span):  # noqa: ANN001
            cls.used_spans.append(span)
            raise AssertionError("use_span should not be called without a stored root span")

    class _FakeEventService:
        def __init__(self) -> None:
            self.stored = SimpleNamespace(id="conv-service-missing")
            self.calls = 0

        async def run(self):  # noqa: ANN201
            self.calls += 1
            return "running"

    monkeypatch.setitem(sys.modules, "lmnr", SimpleNamespace(Laminar=_FakeLaminar))
    _patch_event_service_runtime_span_bridge(_FakeEventService)

    service = _FakeEventService()
    assert asyncio.run(service.run()) == "running"
    assert service.calls == 1


def test_normalize_laminar_environment_maps_aliases_and_http_defaults() -> None:
    env = {
        "LAMINAR_BASE_URL": "http://localhost",
        "LAMINAR_PROJECT_API_KEY": "proj-key",
    }

    normalized = _normalize_laminar_environment(env)

    assert env["LMNR_BASE_URL"] == "http://localhost"
    assert env["LMNR_PROJECT_API_KEY"] == "proj-key"
    assert env["LMNR_HTTP_PORT"] == "8000"
    assert env["LMNR_FORCE_HTTP"] == "1"
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://localhost:8000"
    assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == "http://localhost:8000/v1/traces"
    assert env["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"] == "http://localhost:8000/v1/logs"
    assert env["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"
    assert env["OTEL_EXPORTER"] == "otlp_http"
    assert normalized == {
        "LMNR_BASE_URL": "alias:LAMINAR_BASE_URL",
        "LMNR_PROJECT_API_KEY": "alias:LAMINAR_PROJECT_API_KEY",
        "LMNR_HTTP_PORT": "derived:self-hosted-http-default",
        "LMNR_FORCE_HTTP": "derived:http-scheme",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "derived:lmnr-http-bootstrap",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "derived:lmnr-http-bootstrap",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "derived:lmnr-http-bootstrap",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "derived:lmnr-http-bootstrap",
        "OTEL_EXPORTER": "derived:lmnr-http-bootstrap",
    }


def test_patch_lmnr_http_exporter_defaults_rewrites_force_http_self_hosted_endpoint(
    monkeypatch,
) -> None:
    monkeypatch.setenv("LMNR_BASE_URL", "http://localhost")
    monkeypatch.delenv("LMNR_HTTP_PORT", raising=False)

    from lmnr.opentelemetry_lib.tracing import exporter as lmnr_exporter

    _patch_lmnr_http_exporter_defaults()

    config = lmnr_exporter._configure_exporter(
        "http://localhost",
        None,
        "proj-key",
        30,
        True,
    )
    assert config["endpoint"] == "http://localhost"
    assert config["force_http"] is True


def test_rewrite_localhost_otel_endpoint_url_rewrites_https_localhost(monkeypatch) -> None:
    monkeypatch.setenv("LMNR_BASE_URL", "http://localhost")
    monkeypatch.setenv("LMNR_HTTP_PORT", "80")

    assert (
        _rewrite_localhost_otel_endpoint_url("https://localhost:443/v1/traces")
        == "http://localhost/v1/traces"
    )
    assert (
        _rewrite_localhost_otel_endpoint_url("https://127.0.0.1/v1/logs")
        == "http://localhost/v1/logs"
    )
    assert _rewrite_localhost_otel_endpoint_url("https://api.lmnr.ai/v1/traces") == (
        "https://api.lmnr.ai/v1/traces"
    )


def test_initialize_laminar_runtime_uses_normalized_env(monkeypatch) -> None:
    class _FakeLaminar:
        calls: list[dict[str, object | None]] = []

        @classmethod
        def initialize(cls, **kwargs):  # noqa: ANN003
            cls.calls.append(dict(kwargs))

    monkeypatch.delenv("LMNR_BASE_URL", raising=False)
    monkeypatch.delenv("LMNR_PROJECT_API_KEY", raising=False)
    monkeypatch.delenv("LMNR_HTTP_PORT", raising=False)
    monkeypatch.delenv("LMNR_GRPC_PORT", raising=False)
    monkeypatch.delenv("LMNR_FORCE_HTTP", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_PROTOCOL", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    monkeypatch.setenv("LAMINAR_BASE_URL", "http://localhost")
    monkeypatch.setenv("LAMINAR_PROJECT_API_KEY", "proj-key")
    monkeypatch.setitem(sys.modules, "lmnr", SimpleNamespace(Laminar=_FakeLaminar))

    _initialize_laminar_runtime()

    assert _FakeLaminar.calls == [
        {
            "project_api_key": "proj-key",
            "base_url": "http://localhost",
            "http_port": 8000,
            "grpc_port": None,
            "force_http": True,
        }
    ]
