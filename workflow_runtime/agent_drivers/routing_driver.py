"""Per-step runtime driver router."""

from __future__ import annotations

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.graph_compiler.state_schema import ExecutionBackend


# SEM_BEGIN orchestrator_v1.routing_driver.routing_driver:v1
# type: CLASS
# use_case: Маршрутизирует runtime step в конкретный backend по execution_backend из DriverRequest.
# feature:
#   - TaskUnitRunner сохраняет единый BaseDriver boundary, а выбор реального backend переезжает на уровень step-а
#   - Task card 2026-04-05_1900__oh-laminar-otel-gui, T43
# pre:
#   -
# post:
#   -
# invariant:
#   - router сам не меняет контракт DriverResult и не подменяет payload
# modifies (internal):
#   -
# emits (external):
#   - external.driver_runtime
# errors:
#   - RuntimeError: requested backend is not configured
# depends:
#   - BaseDriver
# sft: route a task unit driver request to the configured per-step backend using execution_backend
# idempotent: false
# logs: -
class RoutingDriver(BaseDriver):
    def __init__(self, *, backends: dict[ExecutionBackend, BaseDriver]) -> None:
        self._backends = dict(backends)

    def run_task(self, request: DriverRequest) -> DriverResult:
        backend = ExecutionBackend(str(request.execution_backend))
        driver = self._backends.get(backend)
        if driver is None:
            raise RuntimeError(f"No runtime driver configured for backend: {backend}")
        return driver.run_task(request)


# SEM_END orchestrator_v1.routing_driver.routing_driver:v1
