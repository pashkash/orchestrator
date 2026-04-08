"""Test helpers for the V1 orchestrator."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable

from workflow_runtime.agent_drivers.base_driver import BaseDriver, DriverRequest, DriverResult
from workflow_runtime.agent_drivers.mock_driver import MockDriver


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
        checklist_items = request.task_context.get("guardrail_prompt_checklists", [])
        checklist_resolutions = [
            {
                "id": str(item.get("id") or ""),
                "status": "done",
                "evidence": "scripted-driver-covered",
            }
            for item in checklist_items
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        ]
        key = (request.phase_id, request.sub_role, request.role_dir)
        self.calls[key] += 1
        queue = self._script.get(key, [])
        if queue:
            item = queue.pop(0)
            if callable(item):
                result = item(request, self.calls[key])
            else:
                result = item
            if checklist_resolutions and "checklist_resolutions" not in result.payload:
                return DriverResult(
                    status=result.status,
                    payload={**result.payload, "checklist_resolutions": checklist_resolutions},
                    raw_text=result.raw_text,
                    conversation_id=result.conversation_id,
                    request_artifact=result.request_artifact,
                    artifact_refs=result.artifact_refs,
                )
            return result
        return self._fallback.run_task(request)
