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
    def __init__(self) -> None:
        self.last_payload = None

    def create_conversation(self, payload, *, trace_id=None):  # noqa: ANN001
        self.last_payload = payload
        return OpenHandsConversationHandle(
            conversation_id="conv-123",
            state={"id": "conv-123", "execution_status": "idle"},
        )

    def run_conversation(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        return {"ok": True}

    def wait_until_finished(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        return {"execution_status": "FINISHED"}

    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "agent",
                "llm_message": {
                    "role": "assistant",
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
                    ],
                },
            }
        ]


def test_openhands_driver_parses_yaml_payload():
    api = FakeOpenHandsApi()
    driver = OpenHandsDriver(
        api=api,
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
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-openhands-driver"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.conversation_id == "conv-123"
    assert result.payload["structured_output"]["subtask_id"] == "subtask-1"
    assert api.last_payload["initial_message"]["run"] is False


class FakeOpenHandsApiError(FakeOpenHandsApi):
    """Simulates an OpenHands session that ends with execution_status=error."""

    def wait_until_finished(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        return {"execution_status": "error"}

    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "user",
                "content": [{"type": "text", "text": "```yaml\nstatus: PASS\n```"}],
            },
            {
                "source": "agent",
                "content": [{"type": "text", "text": "LLMBadRequestError: model not found"}],
            },
        ]


class FakeOpenHandsApiUserYaml(FakeOpenHandsApi):
    """User prompt contains YAML but agent reply does not — must not parse user YAML."""

    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "user",
                "content": [{"type": "text", "text": "```yaml\nstatus: PASS\nsummary: from prompt\n```"}],
            },
            {
                "source": "agent",
                "llm_message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": "```yaml\nstatus: NEEDS_FIX_EXECUTOR\nsummary: real reply\n```",
                        }
                    ],
                },
            },
        ]


class FakeOpenHandsApiSystemPromptAndFinish(FakeOpenHandsApi):
    """System prompt must be ignored; finish message with YAML must be parsed."""

    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "agent",
                "system_prompt": {
                    "text": "You are OpenHands agent.\n```yaml\nstatus: PASS\nsummary: wrong source\n```"
                },
            },
            {
                "source": "agent",
                "action": {
                    "kind": "FinishAction",
                    "message": """```yaml
status: PASS
structured_output:
  task_id: "task-finish"
  subtask_id: "subtask-finish"
  role: "devops"
  status: "done"
  changes: []
  commands_executed: []
  tests_passed: []
  commits: []
  warnings: []
  escalation: null
  summary: "finish yaml"
warnings: []
```""",
                },
            },
        ]


class FakeOpenHandsApiPlainFinish(FakeOpenHandsApi):
    """Plain-text finish must become explicit parse failure."""

    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "agent",
                "system_prompt": {"text": "You are OpenHands agent."},
            },
            {
                "source": "agent",
                "action": {
                    "kind": "FinishAction",
                    "message": "Task completed successfully with plain text summary.",
                },
            },
        ]


def test_openhands_driver_execution_error_forces_needs_fix():
    api = FakeOpenHandsApiError()
    driver = OpenHandsDriver(
        api=api,
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
            model="openrouter/z-ai/glm-5",
            prompt="Do something.",
            task_context={"task_id": "task-err"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-error-guard"},
        )
    )

    assert result.status == PipelineStatus.NEEDS_FIX_EXECUTOR
    assert "execution_status=error" in result.payload["warnings"][0]


def test_openhands_driver_ignores_user_prompt_yaml():
    api = FakeOpenHandsApiUserYaml()
    driver = OpenHandsDriver(
        api=api,
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
            model="openrouter/z-ai/glm-5",
            prompt="Prompt with YAML example.",
            task_context={"task_id": "task-filter"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-filter-user-yaml"},
        )
    )

    assert result.status == PipelineStatus.NEEDS_FIX_EXECUTOR
    assert result.payload["summary"] == "real reply"


def test_openhands_driver_ignores_system_prompt_and_parses_finish_yaml():
    api = FakeOpenHandsApiSystemPromptAndFinish()
    driver = OpenHandsDriver(
        api=api,
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
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block in finish.",
            task_context={"task_id": "task-finish"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-ignore-system-prompt"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["structured_output"]["subtask_id"] == "subtask-finish"
    assert "wrong source" not in result.raw_text


def test_openhands_driver_plain_finish_becomes_parse_failure():
    api = FakeOpenHandsApiPlainFinish()
    driver = OpenHandsDriver(
        api=api,
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
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block in finish.",
            task_context={"task_id": "task-plain-finish"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-plain-finish-parse-failure"},
        )
    )

    assert result.status == PipelineStatus.NEEDS_FIX_EXECUTOR
    assert "non-YAML final output" in result.payload["warnings"][0]


def test_openhands_driver_allows_missing_api_key():
    api = FakeOpenHandsApi()
    driver = OpenHandsDriver(
        api=api,
        llm_api_key=None,
        llm_base_url="https://openrouter.ai/api/v1",
        cli_mode=True,
        tools=list(OPENHANDS_REQUIRED_TOOL_NAMES),
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.EXECUTOR,
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-openhands-driver-no-key"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["structured_output"]["task_id"] == "task-1"
    assert "api_key" not in api.last_payload["agent"]["llm"]


def test_openhands_driver_uses_working_dir_in_payload():
    api = FakeOpenHandsApi()
    driver = OpenHandsDriver(
        api=api,
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        cli_mode=True,
        tools=list(OPENHANDS_REQUIRED_TOOL_NAMES),
    )

    driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.EXECUTOR,
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-working-dir"},
        )
    )

    assert api.last_payload["workspace"]["working_dir"] == "/tmp/task-worktree"
