#!/usr/bin/env python3
"""Wrapper to start OH agent-server with cross-process OTEL trace propagation.

Adds middleware to the already-created FastAPI app (before uvicorn serves
the first request) that captures x-lmnr-parent-ctx header into a ContextVar.
Patches BaseConversation._start_observability_span to use that ContextVar
as parent_span_context, linking OH spans into the orchestrator's trace tree.
"""
import logging
import sys
from contextvars import ContextVar

logger = logging.getLogger("oh_trace_propagation")

_lmnr_parent_ctx: ContextVar[str | None] = ContextVar("_lmnr_parent_ctx", default=None)


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


def _patch_all():
    from openhands.sdk.observability.laminar import (
        should_enable_observability,
        _get_span_manager,
    )
    import openhands.sdk.conversation.base as conv_base

    # OpenHands persists conversation state through LocalFileStore. Those spans are
    # diagnostic noise for our pipeline view and sometimes appear as stray roots.
    _disable_local_filestore_tool_spans()

    _orig_start_span = conv_base.BaseConversation._start_observability_span

    def _patched_start_span(self, session_id: str) -> None:
        if not should_enable_observability():
            return
        parent_ctx_str = _lmnr_parent_ctx.get()
        logger.info(
            "_start_observability_span: session=%s parent_ctx=%s",
            session_id, "present" if parent_ctx_str else "absent",
        )
        if parent_ctx_str:
            try:
                from lmnr import Laminar
                from lmnr.sdk.laminar import LaminarSpanContext
                parent_ctx = LaminarSpanContext.deserialize(parent_ctx_str)
                span = Laminar.start_active_span(
                    "conversation",
                    session_id=session_id,
                    parent_span_context=parent_ctx,
                )
                _get_span_manager()._stack.append(span)
                logger.info("conversation span linked to orchestrator (pushed to SpanManager)")
                return
            except Exception as e:
                logger.error("Link failed: %s", e, exc_info=True)
        _orig_start_span(self, session_id)

    conv_base.BaseConversation._start_observability_span = _patched_start_span

    import openhands.agent_server.api as api_module
    app = api_module.api

    @app.middleware("http")
    async def lmnr_ctx_middleware(request, call_next):
        ctx_header = request.headers.get("x-lmnr-parent-ctx")
        if ctx_header:
            logger.info("MW: got x-lmnr-parent-ctx len=%d path=%s", len(ctx_header), request.url.path)
            token = _lmnr_parent_ctx.set(ctx_header)
            try:
                return await call_next(request)
            finally:
                _lmnr_parent_ctx.reset(token)
        return await call_next(request)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _patch_all()
    from openhands.agent_server.__main__ import main
    sys.exit(main())
