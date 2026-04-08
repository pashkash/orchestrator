"""OpenHands driver normalization tests."""

from __future__ import annotations

from workflow_runtime.agent_drivers.base_driver import DriverRequest
from workflow_runtime.agent_drivers.openhands_driver import OpenHandsDriver
from workflow_runtime.graph_compiler.state_schema import ExecutionBackend, PhaseId, PipelineStatus, SubRole
from workflow_runtime.integrations.openhands_http_api import OpenHandsConversationHandle
from workflow_runtime.integrations.openhands_runtime import (
    OPENHANDS_REQUIRED_TOOL_NAMES,
)


class FakeOpenHandsApi:
    def __init__(self) -> None:
        self.last_payload = None
        self.sent_messages: list[tuple[str, dict]] = []
        self.created_conversations = 0
        self.ran_conversations: list[str] = []

    def create_conversation(self, payload, *, trace_id=None):  # noqa: ANN001
        self.last_payload = payload
        self.created_conversations += 1
        return OpenHandsConversationHandle(
            conversation_id="conv-123",
            state={"id": "conv-123", "execution_status": "idle"},
        )

    def send_message(self, conversation_id, payload, *, trace_id=None):  # noqa: ANN001
        self.sent_messages.append((conversation_id, payload))
        return {"success": True}

    def run_conversation(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        self.ran_conversations.append(conversation_id)
        return {"ok": True}

    def wait_until_finished(self, conversation_id, *, trace_id=None):  # noqa: ANN001
        return {"execution_status": "FINISHED"}

    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "agent",
                "id": "action-file-editor",
                "timestamp": "2026-04-07T10:00:00Z",
                "action": {
                    "kind": "FileEditorAction",
                },
                "tool_name": "file_editor",
                "tool_call_id": "call-file-editor",
                "summary": "Create runtime artifact",
                "reasoning_content": "Creating helper artifact before final finish",
                "kind": "ActionEvent",
            },
            {
                "source": "environment",
                "timestamp": "2026-04-07T10:00:01Z",
                "action_id": "action-file-editor",
                "tool_name": "file_editor",
                "observation": {
                    "content": [{"type": "text", "text": "file created"}],
                    "is_error": False,
                    "kind": "FileEditorObservation",
                },
                "kind": "ObservationEvent",
            },
            {
                "source": "agent",
                "id": "action-finish",
                "timestamp": "2026-04-07T10:00:02Z",
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
                "action": {
                    "kind": "FinishAction",
                    "message": """```yaml
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
                },
                "tool_name": "finish",
                "kind": "ActionEvent",
            },
            {
                "source": "environment",
                "action_id": "action-finish",
                "observation": {
                    "content": [{"type": "text", "text": "finish observed"}],
                    "is_error": False,
                    "kind": "FinishObservation",
                },
            }
        ]


class FakeLaminarSpan:
    def __init__(self, owner, record) -> None:  # noqa: ANN001
        self._owner = owner
        self._record = record

    def end(self) -> None:
        assert self._owner.current[-1] is self._record
        self._owner.current.pop()


class FakeLaminar:
    records: list[dict] = []
    current: list[dict] = []

    @classmethod
    def reset(cls) -> None:
        cls.records = []
        cls.current = []

    @classmethod
    def start_active_span(cls, name: str, input=None, span_type="DEFAULT", **kwargs):  # noqa: ANN001, ANN003
        record = {
            "name": name,
            "input": input,
            "span_type": span_type,
            "kwargs": kwargs,
            "attributes": [],
            "outputs": [],
        }
        cls.records.append(record)
        cls.current.append(record)
        return FakeLaminarSpan(cls, record)

    @classmethod
    def set_span_attributes(cls, attributes: dict) -> None:
        cls.current[-1]["attributes"].append(dict(attributes))

    @classmethod
    def set_span_output(cls, output=None) -> None:  # noqa: ANN001
        cls.current[-1]["outputs"].append(output)


class FakeOpenHandsApiFlatExecutorPayload(FakeOpenHandsApi):
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
task_id: "task-flat"
subtask_id: "subtask-flat"
role: "devops"
status: "done"
changes: []
commands_executed: []
tests_passed: []
commits: []
warnings: []
escalation: null
summary: "flat payload"
checklist_resolutions: []
```""",
                        }
                    ],
                },
            }
        ]


class FakeOpenHandsApiTaggedChecklistPayload(FakeOpenHandsApi):
    def search_events(self, conversation_id, limit=200, *, trace_id=None):  # noqa: ANN001
        return [
            {
                "source": "agent",
                "llm_message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": """<structured_output role="devops">

```yaml
task_id: "task-tagged"
subtask_id: "subtask-tagged"
role: "devops"
status: "done"
changes: []
commands_executed: []
tests_passed: []
commits: []
warnings: []
escalation: null
summary: "tagged structured output"
```

</structured_output>

<checklist_resolutions>
- id: "checklist::common/standards/code_semantic_markup.md::L295"
  status: "not_applicable"
  evidence: "No code changes in this smoke task"
</checklist_resolutions>""",
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
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
    assert api.created_conversations == 1


def test_openhands_driver_wraps_flat_executor_payload():
    api = FakeOpenHandsApiFlatExecutorPayload()
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-flat"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-openhands-flat-payload"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["structured_output"]["subtask_id"] == "subtask-flat"
    assert result.payload["structured_output"]["status"] == "done"
    assert result.payload["checklist_resolutions"] == []
    assert result.payload["warnings"] == []


def test_openhands_driver_parses_tagged_checklist_sections():
    api = FakeOpenHandsApiTaggedChecklistPayload()
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Return executor YAML.",
            task_context={"task_id": "task-tagged"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-openhands-tagged-checklist"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["structured_output"]["subtask_id"] == "subtask-tagged"
    assert result.payload["checklist_resolutions"] == [
        {
            "id": "checklist::common/standards/code_semantic_markup.md::L295",
            "status": "not_applicable",
            "evidence": "No code changes in this smoke task",
        }
    ]


def test_openhands_driver_does_not_emit_synthetic_event_spans_by_default(monkeypatch):
    api = FakeOpenHandsApi()
    FakeLaminar.reset()
    monkeypatch.setattr("workflow_runtime.agent_drivers.openhands_driver.Laminar", FakeLaminar)
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-openhands-event-spans-default-off"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert FakeLaminar.records == []


def test_openhands_driver_emits_synthetic_event_spans_when_fallback_enabled(monkeypatch):
    api = FakeOpenHandsApi()
    FakeLaminar.reset()
    monkeypatch.setattr("workflow_runtime.agent_drivers.openhands_driver.Laminar", FakeLaminar)
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            working_dir="/tmp/task-worktree",
            metadata={
                "trace_id": "test-openhands-event-spans-fallback",
                "emit_synthetic_openhands_fallback_spans": True,
            },
        )
    )

    assert result.status == PipelineStatus.PASS
    span_names = [record["name"] for record in FakeLaminar.records]
    assert "openhands_fallback_step_file_editor" in span_names
    assert "openhands_fallback_step_finish" in span_names
    finish_span = next(record for record in FakeLaminar.records if record["name"] == "openhands_fallback_step_finish")
    assert finish_span["outputs"][-1]["observation_kind"] == "FinishObservation"


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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Return a YAML block.",
            task_context={"task_id": "task-1"},
            working_dir="/tmp/task-worktree",
            metadata={"trace_id": "test-working-dir"},
        )
    )


def test_openhands_driver_reuses_existing_conversation():
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
            execution_backend=ExecutionBackend.OPENHANDS,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Retry with guardrail feedback.",
            task_context={"task_id": "task-reuse"},
            working_dir="/tmp/task-worktree",
            metadata={
                "trace_id": "test-openhands-driver-reuse",
                "reuse_conversation_id": "conv-existing",
            },
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.conversation_id == "conv-existing"
    assert api.created_conversations == 0
    assert api.sent_messages == [
        (
            "conv-existing",
            {
                "role": "user",
                "content": [{"type": "text", "text": "Retry with guardrail feedback."}],
                "run": False,
            },
        )
    ]
    assert api.ran_conversations == ["conv-existing"]
