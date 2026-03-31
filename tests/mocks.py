"""Test helpers for the V1 orchestrator."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable

from squadder_orchestrator.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from squadder_orchestrator.agent_drivers.mock_driver import MockDriver


ScriptFn = Callable[[DriverRequest, int], DriverResult]


class ScriptedDriver(BaseDriver):
    """Queue-based driver for deterministic V1 integration tests."""

    def __init__(
        self,
        script: dict[tuple[str, str, str], list[DriverResult | ScriptFn]],
        *,
        fallback: BaseDriver | None = None,
    ) -> None:
        self._script = {key: list(values) for key, values in script.items()}
        self._fallback = fallback or MockDriver()
        self.calls: dict[tuple[str, str, str], int] = defaultdict(int)

    def run_task(self, request: DriverRequest) -> DriverResult:
        key = (request.phase_id, request.sub_role, request.role_dir)
        self.calls[key] += 1
        queue = self._script.get(key, [])
        if queue:
            item = queue.pop(0)
            if callable(item):
                return item(request, self.calls[key])
            return item
        return self._fallback.run_task(request)
