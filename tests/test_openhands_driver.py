"""OpenHands driver normalization tests."""

from __future__ import annotations

from workflow_runtime.agent_drivers.base_driver import DriverRequest
from workflow_runtime.agent_drivers.openhands_driver import OpenHandsDriver
from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineStatus, SubRole
from workflow_runtime.integrations.openhands_http_api import OpenHandsConversationHandle
from workflow_runtime.integrations.openhands_runtime import (
    OPENHANDS_REQUIRED_TOOL_NAMES,
)


class FakeOpenHandsApi:
    def create_conversation(self, payload, *, trace_id=None):  # noqa: ANN001
        return OpenHandsConversationHandle(
            conversation_id="conv-123",
            state={"id": "conv-123", "execution_status": "idle"},
        )

    def run_conversation(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        return {"ok": True}

    def wait_until_finished(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        return {"execution_status": "FINISHED"}

    def search_events(self, conversation_id, limit=200):  # noqa: ANN001
        return {
            "events": [
                {
                    "content": [
                        {
                            "type": "text",
                            "text": """```yaml
status: PASS
structured_output:
  task_id: "task-1"
  subtask_id: "subtask-1"
  role: "devops"
  status: "done"
  changes:
    - file: "orchestrator/config/flow.yaml"
      type: "modified"
      description: "Updated runtime flow manifest"
  commands_executed: ["uv run pytest tests/ -v"]
  tests_passed: ["test_happy_path"]
  commits: []
  warnings: []
  escalation: null
  summary: "OpenHands parsed output"
warnings: []
```""",
                        }
                    ]
                }
            ]
        }


def test_openhands_driver_parses_yaml_payload():
    driver = OpenHandsDriver(
        api=FakeOpenHandsApi(),
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        cli_mode=True,
        tools=list(OPENHANDS_REQUIRED_TOOL_NAMES),
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.EXECUTOR,
            model="openhands/claude-sonnet-4-5-20250929",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            workspace_root="/root/squadder-devops",
            metadata={"trace_id": "test-openhands-driver"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.conversation_id == "conv-123"
    assert result.payload["structured_output"]["subtask_id"] == "subtask-1"
