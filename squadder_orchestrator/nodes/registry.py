"""Flat node implementation registry.

Maps impl_name (string from flow.yaml) → callable.

Production auto-registers builtins + merge + condition/items fns on import.
LLM-backed implementations (or mocks) are registered externally:
  - Tests: tests/conftest.py registers mocks from tests/mocks.py
  - Production: entrypoint registers OpenHands adapters (Phase 3)

Impl names follow dot-notation matching flow.yaml nodes.impl field:
  "collect.executor"    → collector executor function
  "subtask.reviewer"    → per-subtask reviewer function
  "builtin.human_gate"  → built-in human gate
"""

from __future__ import annotations

from typing import Any, Callable


NodeFn = Callable[..., dict | Any]

_REGISTRY: dict[str, NodeFn] = {}


def register(impl_name: str, fn: NodeFn) -> None:
    """Register or replace a node implementation."""
    _REGISTRY[impl_name] = fn


def get(impl_name: str) -> NodeFn:
    """Look up a node implementation by its flow.yaml impl name."""
    fn = _REGISTRY.get(impl_name)
    if fn is None:
        raise KeyError(
            f"No implementation registered for '{impl_name}'. "
            f"Available: {sorted(_REGISTRY)}"
        )
    return fn


def registered_names() -> list[str]:
    return sorted(_REGISTRY)


# ---------------------------------------------------------------------------
# Auto-register production (deterministic) implementations on import.
# LLM-backed nodes are NOT registered here — see module docstring.
# ---------------------------------------------------------------------------

# SEM_BEGIN orchestrator.registry.bootstrap:v1
# type: METHOD
# brief: Регистрирует production-реализации: builtins (детерминированные ноды),
#   merge (агрегация), condition_fn и items_fn для DSL-маршрутизации.
#   LLM-backed ноды (collect.executor, plan.executor, subtask.* и т.д.)
#   регистрируются внешним кодом: тестами (mocks) или entrypoint-ом (OpenHands).
# pre:
#   - модули builtins, merge, router, conditions доступны для импорта
# post:
#   - builtin.* и merge.handler зарегистрированы в _REGISTRY
#   - condition_fn "has_ready_subtasks" зарегистрирован
#   - items_fn "get_ready_subtasks" зарегистрирован
# invariant: -
# modifies (internal):
#   - registry._REGISTRY
#   - conditions._CONDITION_FNS
#   - conditions._ITEMS_FNS
# emits (external): -
# errors: -
# depends:
#   - nodes.builtins
#   - nodes.merge
#   - nodes.router
#   - conditions
# sft: wire production (deterministic) impl names to callables; register DSL functions
# idempotent: true
# logs: -
def _bootstrap() -> None:
    from squadder_orchestrator.nodes import builtins
    from squadder_orchestrator.nodes.merge import merge_outputs

    register("merge.handler", merge_outputs)

    register("builtin.collect_results", builtins.collect_results)
    register("builtin.human_gate", builtins.human_gate)
    register("builtin.subtask_done", builtins.subtask_done)
    register("builtin.subtask_fail", builtins.subtask_fail)
    register("builtin.increment_retry", builtins.increment_retry)

    from squadder_orchestrator.conditions import register_condition_fn, register_items_fn
    from squadder_orchestrator.nodes.router import get_ready_subtasks

    register_condition_fn(
        "has_ready_subtasks",
        lambda state: len(get_ready_subtasks(state)) > 0,
    )
    register_items_fn("get_ready_subtasks", get_ready_subtasks)
# SEM_END orchestrator.registry.bootstrap:v1


_bootstrap()
