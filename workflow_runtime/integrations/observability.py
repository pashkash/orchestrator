"""Observability helpers for the orchestrator runtime."""

from __future__ import annotations

from contextvars import ContextVar
from uuid import uuid4


_TRACE_ID_CTX: ContextVar[str | None] = ContextVar("orchestrator_trace_id", default=None)


def get_trace_id() -> str | None:
    return _TRACE_ID_CTX.get()


def set_trace_id(trace_id: str) -> None:
    _TRACE_ID_CTX.set(trace_id)


def ensure_trace_id(trace_id: str | None = None) -> str:
    current = trace_id or _TRACE_ID_CTX.get()
    if current:
        return current
    generated = uuid4().hex
    _TRACE_ID_CTX.set(generated)
    return generated
