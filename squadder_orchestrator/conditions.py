"""DSL condition evaluator for YAML-driven graph routing.

Conditions are JSON/YAML objects that describe comparisons on state fields.
The interpreter uses these to build LangGraph routing functions at graph
construction time — no hardcoded field names in graph.py.

Supported forms:
  {field: "x", op: "eq", value: "pass"}         — state["x"] == "pass"
  {field: "a", op: "lt", field_value: "b"}       — state["a"] < state["b"]
  {all: [{...}, {...}]}                          — AND combinator
  {any: [{...}, {...}]}                          — OR combinator
  {fn: "has_ready_subtasks"}                     — registered predicate
"""

from __future__ import annotations

import logging
from typing import Any, Callable


logger = logging.getLogger(__name__)

_OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "lt": lambda a, b: a is not None and b is not None and a < b,
    "gt": lambda a, b: a is not None and b is not None and a > b,
    "le": lambda a, b: a is not None and b is not None and a <= b,
    "ge": lambda a, b: a is not None and b is not None and a >= b,
}

_CONDITION_FNS: dict[str, Callable[[dict], bool]] = {}
_ITEMS_FNS: dict[str, Callable[[dict], list]] = {}


def register_condition_fn(name: str, fn: Callable[[dict], bool]) -> None:
    _CONDITION_FNS[name] = fn


def get_condition_fn(name: str) -> Callable[[dict], bool]:
    fn = _CONDITION_FNS.get(name)
    if fn is None:
        raise KeyError(f"Unknown condition function: '{name}'")
    return fn


def register_items_fn(name: str, fn: Callable[[dict], list]) -> None:
    """Register a fan-out items function by name (referenced in flow.yaml fan_out.items_fn)."""
    _ITEMS_FNS[name] = fn


def get_items_fn(name: str) -> Callable[[dict], list]:
    fn = _ITEMS_FNS.get(name)
    if fn is None:
        raise KeyError(f"Unknown items function: '{name}'. Available: {sorted(_ITEMS_FNS)}")
    return fn


# SEM_BEGIN orchestrator.conditions.evaluate:v1
# type: METHOD
# use_case: Рекурсивно вычисляет DSL-условие из flow.yaml по state-словарю. Поддерживает
#   сравнения полей (eq/ne/lt/gt/le/ge), кросс-полевые сравнения (field_value),
#   булевы комбинаторы (all/any) и зарегистрированные предикат-функции (fn).
# feature:
#   - Маршрутизация графа оркестратора целиком определяется YAML-условиями
#   - docs/common/roles/flow.yaml — edges.routes.when
# pre:
#   - condition содержит одно из: "all", "any", "fn", или ("field" + "op")
#   - если op указан — он из _OPERATORS (eq/ne/lt/gt/le/ge)
#   - если fn указан — он зарегистрирован через register_condition_fn
# post:
#   - возвращает bool — результат вычисления условия
# invariant:
#   - state не мутируется
# modifies (internal): -
# emits (external): -
# errors:
#   - ValueError: pre[1] violated — неизвестный оператор
#   - KeyError: pre[2] violated — незарегистрированная fn
# depends:
#   - _OPERATORS
#   - _CONDITION_FNS
# sft: evaluate DSL condition object against state dict, supporting comparisons, combinators, and registered predicates
# idempotent: true
# logs: -
def evaluate(condition: dict, state: dict) -> bool:
    """Evaluate a DSL condition against a state dict."""
    # === COMBINATOR: all ===
    if "all" in condition:
        return all(evaluate(c, state) for c in condition["all"])

    # === COMBINATOR: any ===
    if "any" in condition:
        return any(evaluate(c, state) for c in condition["any"])

    # === PREDICATE: fn ===
    if "fn" in condition:
        return get_condition_fn(condition["fn"])(state)

    # === COMPARISON: field + op ===
    field = condition["field"]
    op_name = condition["op"]

    # === PRE[1]: op in _OPERATORS ===
    op_fn = _OPERATORS.get(op_name)
    if op_fn is None:
        raise ValueError(f"Unknown operator: '{op_name}'. Valid: {list(_OPERATORS)}")

    field_val = state.get(field)

    if "field_value" in condition:
        compare_to = state.get(condition["field_value"])
    else:
        compare_to = condition["value"]

    return op_fn(field_val, compare_to)
# SEM_END orchestrator.conditions.evaluate:v1
