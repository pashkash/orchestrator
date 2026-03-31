"""Observability helpers for the orchestrator runtime."""

from __future__ import annotations

from contextvars import ContextVar
from uuid import uuid4


_TRACE_ID_CTX: ContextVar[str | None] = ContextVar("orchestrator_trace_id", default=None)


# SEM_BEGIN orchestrator_v1.observability.get_trace_id:v1
# type: METHOD
# use_case: Reads the currently bound runtime trace id from ContextVar storage.
# feature:
#   - Logging helpers and runtime components can reuse one ambient trace id without passing it through every call site
# pre:
#   -
# post:
#   - returns the currently bound trace id or None
# invariant:
#   - context storage is not mutated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - ContextVar
# sft: read the current orchestrator trace id from context variable storage
# idempotent: true
# logs: -
def get_trace_id() -> str | None:
    return _TRACE_ID_CTX.get()


# SEM_END orchestrator_v1.observability.get_trace_id:v1


# SEM_BEGIN orchestrator_v1.observability.set_trace_id:v1
# type: METHOD
# use_case: Binds an explicit trace id into the current runtime context.
# feature:
#   - Runtime entrypoints can seed one trace id that downstream logs and helpers reuse
# pre:
#   - trace_id is not empty
# post:
#   - the provided trace id becomes the current context trace id
# invariant:
#   - only ContextVar storage is updated
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - ContextVar
# sft: bind an explicit trace id into the current orchestrator context storage
# idempotent: false
# logs: -
def set_trace_id(trace_id: str) -> None:
    _TRACE_ID_CTX.set(trace_id)


# SEM_END orchestrator_v1.observability.set_trace_id:v1


# SEM_BEGIN orchestrator_v1.observability.ensure_trace_id:v1
# type: METHOD
# use_case: Resolves the active trace id from an explicit value or generates a new runtime trace id.
# feature:
#   - AFL logging requires one stable trace id across phase wrappers driver calls and HTTP polling
#   - Task card 2026-03-24_1800__multi-agent-system-design, observability support for D3-D5
# pre:
#   -
# post:
#   - returns a non-empty trace id string
#   - stores the resolved trace id in ContextVar when generation is required
# invariant:
#   - an already-bound trace id is preserved
# modifies (internal):
#   -
# emits (external):
#   -
# errors:
#   -
# depends:
#   - uuid4
#   - ContextVar
# sft: resolve or generate a stable trace id for orchestrator runtime logging
# idempotent: false
# logs: -
def ensure_trace_id(trace_id: str | None = None) -> str:
    current = trace_id or _TRACE_ID_CTX.get()
    if current:
        return current
    generated = uuid4().hex
    _TRACE_ID_CTX.set(generated)
    return generated


# SEM_END orchestrator_v1.observability.ensure_trace_id:v1
