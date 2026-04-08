"""Tests for direct LLM and LangChain tool runtime backends."""

from __future__ import annotations

from pathlib import Path
import time

from workflow_runtime.agent_drivers.base_driver import DriverRequest
from workflow_runtime.agent_drivers.direct_llm_driver import DirectLlmDriver
from workflow_runtime.agent_drivers.langchain_tools_driver import LangChainToolsDriver
from workflow_runtime.graph_compiler.state_schema import ExecutionBackend, PhaseId, PipelineStatus, SubRole
from workflow_runtime.integrations import phase_config_loader


class FakeResponse:
    def __init__(self, *, content: str, tool_calls: list[dict] | None = None) -> None:
        self.content = content
        self.tool_calls = tool_calls or []
        self.additional_kwargs = {}


class FakeDirectChatModel:
    last_init: dict | None = None

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs
        FakeDirectChatModel.last_init = kwargs

    def invoke(self, messages):  # noqa: ANN001
        return FakeResponse(content="```yaml\nstatus: PASS\nfeedback: ok\nwarnings: []\n```")


class FakeToolChatModel:
    last_init: dict | None = None
    calls = 0

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs
        FakeToolChatModel.last_init = kwargs

    def bind_tools(self, tools):  # noqa: ANN001
        self.tools = tools
        return self

    def invoke(self, messages):  # noqa: ANN001
        FakeToolChatModel.calls += 1
        has_tool_message = any(message.__class__.__name__ == "ToolMessage" for message in messages)
        if not has_tool_message:
            return FakeResponse(
                content="",
                tool_calls=[{"id": "call-1", "name": "read_file", "args": {"path": "input.txt"}}],
            )
        return FakeResponse(content="```yaml\nstatus: PASS\nresult: tool flow ok\ntests_passed: []\nwarnings: []\n```")


class FakeRepairDirectChatModel:
    calls = 0

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs

    def invoke(self, messages):  # noqa: ANN001
        FakeRepairDirectChatModel.calls += 1
        if FakeRepairDirectChatModel.calls == 1:
            return FakeResponse(content="```yaml\nstatus: PASS\nwarnings: []\n```")
        return FakeResponse(content="```yaml\nstatus: PASS\nfeedback: repaired\nwarnings: []\n```")


class FakeChecklistRepairDirectChatModel:
    calls = 0

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs

    def invoke(self, messages):  # noqa: ANN001
        FakeChecklistRepairDirectChatModel.calls += 1
        if FakeChecklistRepairDirectChatModel.calls == 1:
            return FakeResponse(content="```yaml\nstatus: PASS\nfeedback: ok\nwarnings: []\n```")
        return FakeResponse(
            content="""```yaml
status: PASS
feedback: ok
issues: []
checklist_resolutions:
  - id: "checklist::common/roles/supervisor/reviewer.md::L60"
    status: "done"
    evidence: "Checklist item reviewed during repair pass"
warnings: []
```"""
        )


class FakeFlatExecutorDirectChatModel:
    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs

    def invoke(self, messages):  # noqa: ANN001
        return FakeResponse(
            content="""```yaml
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
summary: "flat executor payload"
checklist_resolutions: []
```"""
        )


class FakeTimeoutThenSuccessDirectChatModel:
    calls = 0

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs

    def invoke(self, messages):  # noqa: ANN001
        FakeTimeoutThenSuccessDirectChatModel.calls += 1
        if FakeTimeoutThenSuccessDirectChatModel.calls < 3:
            raise TimeoutError("provider timed out")
        return FakeResponse(content="```yaml\nstatus: PASS\nfeedback: recovered\nwarnings: []\n```")


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


class FakeRepairToolChatModel:
    calls = 0

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs

    def bind_tools(self, tools):  # noqa: ANN001
        self.tools = tools
        return self

    def invoke(self, messages):  # noqa: ANN001
        FakeRepairToolChatModel.calls += 1
        if FakeRepairToolChatModel.calls == 1:
            return FakeResponse(
                content="",
                tool_calls=[{"id": "call-1", "name": "read_file", "args": {"path": "input.txt"}}],
            )
        if FakeRepairToolChatModel.calls == 2:
            return FakeResponse(content="```yaml\nstatus: PASS\nwarnings: []\n```")
        return FakeResponse(content="```yaml\nstatus: PASS\ncurrent_state:\n  repo: ok\nwarnings: []\n```")


class FakeFlatTesterToolChatModel:
    calls = 0

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs

    def bind_tools(self, tools):  # noqa: ANN001
        self.tools = tools
        return self

    def invoke(self, messages):  # noqa: ANN001
        FakeFlatTesterToolChatModel.calls += 1
        has_tool_message = any(message.__class__.__name__ == "ToolMessage" for message in messages)
        if not has_tool_message:
            return FakeResponse(
                content="",
                tool_calls=[{"id": "call-1", "name": "read_file", "args": {"path": "input.txt"}}],
            )
        return FakeResponse(
            content="""```yaml
task_id: "task-flat"
subtask_id: "subtask-flat"
role: "devops"
status: "done"
changes: []
commands_executed:
  - "cat input.txt"
tests_passed:
  - "input.txt exists"
  - "input.txt contains expected content"
commits: []
warnings: []
escalation: null
summary: "Smoke verification completed"
```"""
        )


def test_direct_llm_driver_parses_yaml(monkeypatch):
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeDirectChatModel,
    )
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.PLAN,
            role_dir="supervisor",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_plan",
            model="openrouter/z-ai/glm-5",
            prompt="Return review YAML.",
            task_context={},
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-llm"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["feedback"] == "ok"
    assert FakeDirectChatModel.last_init["model"] == "z-ai/glm-5"


def test_direct_llm_driver_repairs_missing_checklist_resolutions(monkeypatch):
    FakeChecklistRepairDirectChatModel.calls = 0
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeChecklistRepairDirectChatModel,
    )
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.PLAN,
            role_dir="supervisor",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_plan",
            model="openrouter/z-ai/glm-5",
            prompt="Return review YAML.",
            task_context={
                "guardrail_prompt_checklists": [
                    {
                        "id": "checklist::common/roles/supervisor/reviewer.md::L60",
                        "text": "- [ ] Example planner review item",
                    }
                ]
            },
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-llm-checklist-repair"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["feedback"] == "ok"
    assert result.payload["checklist_resolutions"] == [
        {
            "id": "checklist::common/roles/supervisor/reviewer.md::L60",
            "status": "done",
            "evidence": "Checklist item reviewed during repair pass",
        }
    ]
    assert FakeChecklistRepairDirectChatModel.calls == 2


def test_direct_llm_driver_wraps_flat_executor_payload(monkeypatch):
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeFlatExecutorDirectChatModel,
    )
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.EXECUTOR,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="task_worker",
            model="openrouter/z-ai/glm-5",
            prompt="Return executor YAML.",
            task_context={},
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-flat-executor"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["structured_output"]["subtask_id"] == "subtask-flat"
    assert result.payload["structured_output"]["status"] == "done"
    assert result.payload["checklist_resolutions"] == []


def test_langchain_tools_driver_executes_read_file_tool(monkeypatch, tmp_path: Path):
    FakeToolChatModel.calls = 0
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.langchain_tools_driver.ChatOpenAI",
        FakeToolChatModel,
    )
    (tmp_path / "input.txt").write_text("hello runtime")
    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.COLLECT,
            role_dir="collector",
            sub_role=SubRole.TESTER,
            execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
            execution_strategy="runtime_validation",
            model="openrouter/z-ai/glm-5",
            prompt="Read input.txt and return tester YAML.",
            task_context={
                "task_worktree_root": str(tmp_path),
                "task_dir_path": str(tmp_path),
                "source_workspace_root": str(tmp_path),
            },
            working_dir=str(tmp_path),
            metadata={"trace_id": "test-langchain-tools"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["result"] == "tool flow ok"
    assert FakeToolChatModel.last_init["model"] == "z-ai/glm-5"
    assert FakeToolChatModel.calls == 2


def test_langchain_tools_driver_records_provider_turn_spans(monkeypatch, tmp_path: Path):
    FakeToolChatModel.calls = 0
    FakeLaminar.reset()
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.langchain_tools_driver.ChatOpenAI",
        FakeToolChatModel,
    )
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.langchain_tools_driver.Laminar",
        FakeLaminar,
    )
    (tmp_path / "input.txt").write_text("hello runtime")
    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.COLLECT,
            role_dir="collector",
            sub_role=SubRole.TESTER,
            execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
            execution_strategy="runtime_validation",
            model="openrouter/z-ai/glm-5",
            prompt="Read input.txt and return tester YAML.",
            task_context={
                "task_worktree_root": str(tmp_path),
                "task_dir_path": str(tmp_path),
                "source_workspace_root": str(tmp_path),
            },
            working_dir=str(tmp_path),
            metadata={"trace_id": "test-langchain-turn-spans"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert [record["name"] for record in FakeLaminar.records] == [
        "langchain_tools_provider_turn",
        "langchain_tools_provider_turn",
    ]
    assert FakeLaminar.records[0]["outputs"][-1]["status"] == "success"
    assert FakeLaminar.records[0]["outputs"][-1]["tool_call_count"] == 1
    assert FakeLaminar.records[1]["outputs"][-1]["status"] == "success"
    assert FakeLaminar.records[1]["outputs"][-1]["tool_call_count"] == 0


def test_langchain_tools_driver_normalizes_flat_tester_payload(monkeypatch, tmp_path: Path):
    FakeFlatTesterToolChatModel.calls = 0
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.langchain_tools_driver.ChatOpenAI",
        FakeFlatTesterToolChatModel,
    )
    (tmp_path / "input.txt").write_text("hello runtime")
    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.TESTER,
            execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
            execution_strategy="runtime_validation",
            model="openrouter/z-ai/glm-5",
            prompt="Read input.txt and return tester YAML.",
            task_context={
                "task_worktree_root": str(tmp_path),
                "task_dir_path": str(tmp_path),
                "source_workspace_root": str(tmp_path),
            },
            working_dir=str(tmp_path),
            metadata={"trace_id": "test-langchain-flat-tester"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["result"]["tests"] == [
        {"name": "check_1", "status": "pass", "output": "input.txt exists"},
        {"name": "check_2", "status": "pass", "output": "input.txt contains expected content"},
    ]
    assert result.payload["result"]["diagnostics"] is None
    assert result.payload["feedback"] == "Smoke verification completed"
    assert FakeFlatTesterToolChatModel.calls == 2


def test_langchain_tools_driver_resolves_technical_docs_alias(monkeypatch, tmp_path: Path):
    docs_root = tmp_path / "docs"
    target_file = docs_root / "common" / "common_rules.md"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("rules text")
    workspace_file = tmp_path / "workspace.code-workspace"
    workspace_file.write_text(
        """{
  "folders": [
    {
      "name": "📝 Technical Docs",
      "path": "%s"
    }
  ]
}"""
        % str(docs_root)
    )
    monkeypatch.setattr(phase_config_loader, "WORKSPACE_CONFIG_PATH", workspace_file)
    phase_config_loader.get_workspace_folder_map.cache_clear()
    phase_config_loader.get_runtime_alias_map.cache_clear()
    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )
    request = DriverRequest(
        phase_id=PhaseId.COLLECT,
        role_dir="collector",
        sub_role=SubRole.EXECUTOR,
        execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
        execution_strategy="collector_context_builder",
        model="openrouter/z-ai/glm-5",
        prompt="unused",
        task_context={
            "task_worktree_root": str(tmp_path),
            "task_dir_path": str(tmp_path),
            "source_workspace_root": str(tmp_path),
            "methodology_root_runtime": str(docs_root),
        },
        working_dir=str(tmp_path),
        metadata={"trace_id": "test-docs-alias"},
    )

    assert driver._tool_read_file(path="Technical Docs/common/common_rules.md", request=request) == "rules text"
    duplicated_absolute = str(docs_root / "Technical Docs" / "common" / "common_rules.md")
    assert driver._tool_read_file(path=duplicated_absolute, request=request) == "rules text"
    phase_config_loader.get_workspace_folder_map.cache_clear()
    phase_config_loader.get_runtime_alias_map.cache_clear()


def test_langchain_tools_driver_resolves_task_docs_absolute_alias(tmp_path: Path):
    docs_root = tmp_path / "task-docs"
    target_file = docs_root / "common" / "common_rules.md"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("rules text")

    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )
    request = DriverRequest(
        phase_id=PhaseId.COLLECT,
        role_dir="collector",
        sub_role=SubRole.EXECUTOR,
        execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
        execution_strategy="collector_context_builder",
        model="openrouter/z-ai/glm-5",
        prompt="unused",
        task_context={
            "task_worktree_root": str(tmp_path),
            "task_dir_path": str(tmp_path),
            "source_workspace_root": str(tmp_path),
            "methodology_root_runtime": str(docs_root),
        },
        working_dir=str(tmp_path),
        metadata={"trace_id": "test-task-docs-alias"},
    )

    duplicated_absolute = str(docs_root / "Technical Docs" / "common" / "common_rules.md")
    assert driver._tool_read_file(path=duplicated_absolute, request=request) == "rules text"


def test_langchain_tools_driver_resolves_symlinked_task_docs_absolute_alias(tmp_path: Path):
    source_docs_root = tmp_path / "source-docs"
    target_file = source_docs_root / "common" / "common_rules.md"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("rules text")
    task_dir = tmp_path / "task-dir"
    task_dir.mkdir(parents=True, exist_ok=True)
    symlinked_docs_root = task_dir / "docs"
    symlinked_docs_root.symlink_to(source_docs_root, target_is_directory=True)

    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )
    request = DriverRequest(
        phase_id=PhaseId.COLLECT,
        role_dir="collector",
        sub_role=SubRole.EXECUTOR,
        execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
        execution_strategy="collector_context_builder",
        model="openrouter/z-ai/glm-5",
        prompt="unused",
        task_context={
            "task_worktree_root": str(task_dir / "workspace"),
            "task_dir_path": str(task_dir),
            "source_workspace_root": str(tmp_path),
            "methodology_root_runtime": str(symlinked_docs_root),
        },
        working_dir=str(task_dir),
        metadata={"trace_id": "test-symlink-task-docs-alias"},
    )

    duplicated_absolute = str(symlinked_docs_root / "Technical Docs" / "common" / "common_rules.md")
    assert driver._tool_read_file(path=duplicated_absolute, request=request) == "rules text"


def test_langchain_tools_driver_allows_source_workspace_roots_for_read_and_glob(tmp_path: Path):
    task_root = tmp_path / "task-root"
    source_root = tmp_path / "source-root"
    task_root.mkdir(parents=True, exist_ok=True)
    (source_root / "docs").mkdir(parents=True, exist_ok=True)
    target_file = source_root / "docs" / "note.md"
    target_file.write_text("source workspace file")

    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )
    request = DriverRequest(
        phase_id=PhaseId.COLLECT,
        role_dir="collector",
        sub_role=SubRole.EXECUTOR,
        execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
        execution_strategy="collector_context_builder",
        model="openrouter/z-ai/glm-5",
        prompt="unused",
        task_context={
            "task_worktree_root": str(task_root),
            "task_dir_path": str(task_root),
            "source_workspace_root": str(source_root),
            "source_workspace_roots": {"devops": str(source_root)},
        },
        working_dir=str(task_root),
        metadata={"trace_id": "test-source-workspace-roots"},
    )

    assert driver._tool_read_file(path=str(target_file), request=request) == "source workspace file"
    glob_output = driver._tool_glob(pattern="**/*.md", target_directory=str(source_root), request=request)
    assert str(target_file) in glob_output


def test_direct_llm_driver_repairs_missing_required_keys(monkeypatch):
    FakeRepairDirectChatModel.calls = 0
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeRepairDirectChatModel,
    )
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.PLAN,
            role_dir="supervisor",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_plan",
            model="openrouter/z-ai/glm-5",
            prompt="Return review YAML.",
            task_context={},
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-repair"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["feedback"] == "repaired"
    assert FakeRepairDirectChatModel.calls == 2


def test_direct_llm_driver_retries_timeout_then_succeeds(monkeypatch):
    FakeTimeoutThenSuccessDirectChatModel.calls = 0
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeTimeoutThenSuccessDirectChatModel,
    )
    monkeypatch.setattr("workflow_runtime.agent_drivers.direct_llm_driver.time.sleep", lambda _: None)
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.PLAN,
            role_dir="supervisor",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_plan",
            model="openrouter/z-ai/glm-5",
            prompt="Return review YAML.",
            task_context={},
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-timeout-retry"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["feedback"] == "recovered"
    assert FakeTimeoutThenSuccessDirectChatModel.calls == 3


def test_direct_llm_driver_applies_execution_runtime_overrides(monkeypatch):
    captured: dict[str, int] = {}
    FakeDirectChatModel.last_init = None
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeDirectChatModel,
    )

    def _fake_invoke_with_retry(  # noqa: ANN001
        self,
        *,
        llm_factory,
        messages,
        hard_timeout_seconds,
        idle_timeout_seconds,
        trace_id,
        phase_id,
        role_dir,
        sub_role,
        call_kind,
    ):
        del self, messages, trace_id, phase_id, role_dir, sub_role, call_kind
        llm_factory()
        captured["hard_timeout_seconds"] = hard_timeout_seconds
        captured["idle_timeout_seconds"] = idle_timeout_seconds
        return FakeResponse(content="```yaml\nstatus: PASS\nfeedback: ok\nwarnings: []\n```")

    monkeypatch.setattr(DirectLlmDriver, "_invoke_with_retry", _fake_invoke_with_retry)
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_worker_result",
            model="openrouter/z-ai/glm-5",
            prompt="Return review YAML.",
            task_context={},
            working_dir="/tmp",
            metadata={
                "trace_id": "test-direct-timeout-overrides",
                "execution_runtime_overrides": {
                    "timeout_seconds": 300,
                    "idle_timeout_seconds": 45,
                },
            },
        )
    )

    assert result.status == PipelineStatus.PASS
    assert captured == {
        "hard_timeout_seconds": 300,
        "idle_timeout_seconds": 45,
    }
    assert FakeDirectChatModel.last_init["timeout"] == 300


def test_direct_llm_driver_relaxes_timeouts_for_large_prompts(monkeypatch):
    captured: dict[str, int] = {}
    FakeDirectChatModel.last_init = None
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeDirectChatModel,
    )

    def _fake_invoke_with_retry(  # noqa: ANN001
        self,
        *,
        llm_factory,
        messages,
        hard_timeout_seconds,
        idle_timeout_seconds,
        trace_id,
        phase_id,
        role_dir,
        sub_role,
        call_kind,
    ):
        del self, messages, trace_id, phase_id, role_dir, sub_role, call_kind
        llm_factory()
        captured["hard_timeout_seconds"] = hard_timeout_seconds
        captured["idle_timeout_seconds"] = idle_timeout_seconds
        return FakeResponse(content="```yaml\nstatus: PASS\nfeedback: ok\nwarnings: []\n```")

    monkeypatch.setattr(DirectLlmDriver, "_invoke_with_retry", _fake_invoke_with_retry)
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=120,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.PLAN,
            role_dir="supervisor",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_plan",
            model="openrouter/z-ai/glm-5",
            prompt="x" * 82000,
            task_context={},
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-large-prompt-timeouts"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert captured == {
        "hard_timeout_seconds": 240,
        "idle_timeout_seconds": 30,
    }
    assert FakeDirectChatModel.last_init["timeout"] == 240


def test_direct_llm_driver_records_attempt_and_backoff_spans(monkeypatch):
    FakeTimeoutThenSuccessDirectChatModel.calls = 0
    FakeLaminar.reset()
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.ChatOpenAI",
        FakeTimeoutThenSuccessDirectChatModel,
    )
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.Laminar",
        FakeLaminar,
    )
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.direct_llm_driver.time.sleep",
        lambda seconds: None,
    )
    driver = DirectLlmDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_attempts=3,
        retry_backoff_seconds=1,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.EXECUTE,
            role_dir="devops",
            sub_role=SubRole.REVIEWER,
            execution_backend=ExecutionBackend.DIRECT_LLM,
            execution_strategy="review_changes",
            model="openrouter/z-ai/glm-5",
            prompt="Return review YAML.",
            task_context={},
            working_dir="/tmp",
            metadata={"trace_id": "test-direct-attempt-span"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert [record["name"] for record in FakeLaminar.records] == [
        "direct_llm_provider_attempt",
        "direct_llm_retry_backoff",
        "direct_llm_provider_attempt",
        "direct_llm_retry_backoff",
        "direct_llm_provider_attempt",
    ]
    assert FakeLaminar.records[0]["outputs"][-1]["status"] == "error"
    assert FakeLaminar.records[1]["outputs"][-1]["status"] == "slept"
    assert FakeLaminar.records[2]["outputs"][-1]["status"] == "error"
    assert FakeLaminar.records[3]["outputs"][-1]["status"] == "slept"
    assert FakeLaminar.records[4]["outputs"][-1]["status"] == "success"


def test_langchain_tools_driver_repairs_missing_required_keys(monkeypatch, tmp_path: Path):
    FakeRepairToolChatModel.calls = 0
    monkeypatch.setattr(
        "workflow_runtime.agent_drivers.langchain_tools_driver.ChatOpenAI",
        FakeRepairToolChatModel,
    )
    (tmp_path / "input.txt").write_text("hello runtime")
    driver = LangChainToolsDriver(
        llm_api_key="test-key",
        llm_base_url="https://openrouter.ai/api/v1",
        timeout_seconds=30,
        max_iterations=4,
        shell_timeout_seconds=5,
        max_output_chars=4000,
    )

    result = driver.run_task(
        DriverRequest(
            phase_id=PhaseId.COLLECT,
            role_dir="collector",
            sub_role=SubRole.EXECUTOR,
            execution_backend=ExecutionBackend.LANGCHAIN_TOOLS,
            execution_strategy="collector_context_builder",
            model="openrouter/z-ai/glm-5",
            prompt="Read input.txt and return collector YAML.",
            task_context={
                "task_worktree_root": str(tmp_path),
                "task_dir_path": str(tmp_path),
                "source_workspace_root": str(tmp_path),
            },
            working_dir=str(tmp_path),
            metadata={"trace_id": "test-tool-repair"},
        )
    )

    assert result.status == PipelineStatus.PASS
    assert result.payload["current_state"]["repo"] == "ok"
    assert FakeRepairToolChatModel.calls == 3
