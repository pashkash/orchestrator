"""V1 phase-driven flow tests."""

from __future__ import annotations

from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from workflow_runtime.integrations import phase_config_loader
from workflow_runtime.agent_drivers.base_driver import DriverResult
from workflow_runtime.agent_drivers.mock_driver import MockDriver
from workflow_runtime.graph_compiler.langgraph_builder import _build_driver, compile_graph
from workflow_runtime.graph_compiler.state_schema import ExecutionBackend, PhaseId, PipelineStatus
from workflow_runtime.integrations.phase_config_loader import (
    get_flow_manifest,
    get_runtime_config,
    load_all_role_metadata,
)
from workflow_runtime.node_implementations.phases.collect_phase import run_collect_phase
from workflow_runtime.node_implementations.phases.plan_phase import run_plan_phase
from workflow_runtime.node_implementations.task_unit import TaskUnitRunner
from workflow_runtime.integrations.prompt_composer import compose_prompt
from tests.mocks import ScriptedDriver


def test_happy_path_completes_with_mock_driver(initial_state):
    app = compile_graph(driver=MockDriver())
    result = app.invoke(initial_state)

    assert result["current_status"] == "PASS"
    assert result["final_result"] == "Mock validation succeeded"
    assert len(result["plan"]) == 2
    assert all(subtask.status == "done" for subtask in result["plan"])
    assert len(result["structured_outputs"]) == 2


def test_execute_respects_dependencies(initial_state):
    app = compile_graph(driver=MockDriver())
    result = app.invoke(initial_state)

    by_id = {subtask.id: subtask for subtask in result["plan"]}
    assert by_id["backend-wire-phase-runtime"].dependencies == ["devops-update-runtime-config"]
    assert by_id["backend-wire-phase-runtime"].status == "done"


def test_graph_nodes_match_runtime_manifest():
    flow = get_flow_manifest()
    app = compile_graph(driver=MockDriver())

    graph_nodes = {name for name in app.get_graph().nodes.keys() if not name.startswith("__")}
    manifest_nodes = {phase.id for phase in flow.phases}
    assert graph_nodes == manifest_nodes


def test_runtime_loaders_reflect_v1_defaults():
    roles = load_all_role_metadata()
    runtime = get_runtime_config()
    flow = get_flow_manifest()

    assert "devops" in roles
    assert "collector" in roles
    assert "common/standards/code_semantic_markup.md" in roles["devops"].force_injected_documents
    assert "project_specific/AGENTS_PROJECT.md" in roles["collector"].force_injected_documents
    assert "common/standards/guides_semantic_markup.md" in roles["architect"].force_injected_documents
    assert "common/templates/adr_template.md" in roles["architect"].force_injected_documents
    assert runtime.phases["execute"].strategy.max_concurrent == 1
    assert runtime.methodology_root_default == "/root/squadder-devops/docs"
    assert runtime.methodology_agents_entrypoint == "AGENTS.md"
    assert runtime.role_metadata_path == "Technical Docs/common/roles/{role_dir}/role.yaml"
    assert "Technical Docs/common/common_rules.md" in runtime.force_injected_common_documents
    assert [repo.id for repo in runtime.task_repositories] == ["devops", "backend-prod"]
    assert runtime.task_repositories[0].source_repo_root == "/root/squadder-devops"
    assert runtime.task_repositories[0].default_sparse_paths == [
        "orchestrator",
        "docs",
        "business-docs",
    ]
    assert runtime.task_repositories[1].source_repo_root == "/root/dev-prod-squadder/app"
    assert runtime.task_repositories[1].default_for_roles == ["backend"]
    assert runtime.openhands["base_url_default"] == "http://127.0.0.1:8011"
    assert runtime.openhands["methodology_root_runtime"] == "/root/squadder-devops/docs"
    assert runtime.openhands["max_poll_interval_seconds"] == 15
    assert runtime.openhands["poll_log_every_n_attempts"] == 5
    assert runtime.openhands["timeout_seconds"] == 360
    assert runtime.direct_llm["llm_base_url"] == "https://openrouter.ai/api/v1"
    assert runtime.direct_llm["timeout_seconds"] == 120
    assert runtime.direct_llm["idle_timeout_seconds"] == 15
    assert runtime.direct_llm["max_attempts"] == 3
    assert runtime.direct_llm["retry_backoff_seconds"] == 2
    assert runtime.langchain_tools["max_iterations"] == 8
    assert runtime.phases["plan"].pipeline.executor.execution.backend == ExecutionBackend.DIRECT_LLM
    assert runtime.phases["plan"].pipeline.reviewer.guardrails == ["ensure_checklist"]
    assert runtime.phases["execute"].default_worker_pipeline.reviewer.guardrails == ["ensure_checklist"]
    assert runtime.phases["execute"].default_worker_pipeline.reviewer.execution.runtime_overrides == {
        "timeout_seconds": 300,
        "idle_timeout_seconds": 45,
    }
    assert runtime.phases["execute"].default_worker_pipeline.tester.guardrails == ["ensure_checklist"]
    assert runtime.phases["execute"].default_worker_pipeline.executor.execution.backend == ExecutionBackend.OPENHANDS
    assert flow.version == "1.0"
    assert flow.start_phase == "collect"


def test_resolve_runtime_path_uses_workspace_mapping(monkeypatch, tmp_path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
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

    resolved = phase_config_loader.resolve_runtime_path("Technical Docs/common/roles/collector/executor.md")

    assert resolved == docs_root / "common" / "roles" / "collector" / "executor.md"

    phase_config_loader.get_workspace_folder_map.cache_clear()
    phase_config_loader.get_runtime_alias_map.cache_clear()


def test_build_driver_uses_runtime_openhands_base_url_default(monkeypatch):
    runtime = get_runtime_config()
    monkeypatch.delenv("OPENHANDS_BASE_URL", raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")

    driver = _build_driver("live", runtime)

    assert driver._backends[ExecutionBackend.OPENHANDS]._api._base_url == "http://127.0.0.1:8011"
    assert driver._backends[ExecutionBackend.DIRECT_LLM]._max_attempts == 3
    assert driver._backends[ExecutionBackend.DIRECT_LLM]._retry_backoff_seconds == 2


def test_build_driver_accepts_deprecated_openhands_alias(monkeypatch):
    runtime = get_runtime_config()
    monkeypatch.delenv("OPENHANDS_BASE_URL", raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")

    driver = _build_driver("openhands", runtime)

    assert driver._backends[ExecutionBackend.OPENHANDS]._api._base_url == "http://127.0.0.1:8011"


def test_build_driver_fails_fast_when_hybrid_api_key_missing(monkeypatch):
    runtime = get_runtime_config()
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    try:
        _build_driver("live", runtime)
    except ValueError as exc:
        assert "Missing direct LLM API key" in str(exc)
    else:
        raise AssertionError("Expected _build_driver to fail without OPENROUTER_API_KEY")


def test_prompt_builder_loads_role_prompt_and_runtime_sections():
    runtime = get_runtime_config()
    execute_prompt = compose_prompt(
        phase_id=PhaseId.EXECUTE,
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.executor,
        task_context={},
    )
    collect_prompt = compose_prompt(
        phase_id=PhaseId.COLLECT,
        role_dir="collector",
        step_config=runtime.phases["collect"].pipeline.executor,
        task_context={},
    )
    plan_prompt = compose_prompt(
        phase_id=PhaseId.PLAN,
        role_dir="supervisor",
        step_config=runtime.phases["plan"].pipeline.executor,
        task_context={},
    )
    review_prompt = compose_prompt(
        phase_id=PhaseId.EXECUTE,
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.reviewer,
        task_context={
            "executor_payload": {
                "status": "PASS",
                "structured_output": {"summary": "square.py updated"},
            }
        },
    )
    assert "# Role: devops / executor" in execute_prompt
    assert "executor_common.md" in execute_prompt
    assert "## Force-Injected Documents" in execute_prompt
    assert "## Checklist Guardrail Items" in execute_prompt
    assert "## Dynamic Document Candidates" not in execute_prompt
    assert "AGENTS_PROJECT.md" in execute_prompt
    assert "DEVOPS_GUIDE.md" in execute_prompt
    assert "code_semantic_markup.md" in execute_prompt
    assert "ai_friendly_logging_markup.md" in execute_prompt
    assert "checklist_resolutions" in execute_prompt
    assert "Runtime Task Context" in execute_prompt
    assert "Output Contract" in execute_prompt
    assert "## Force-Injected Documents" in collect_prompt
    assert "## Dynamic Document Candidates" not in collect_prompt
    assert "`/root/squadder-devops/docs/AGENTS.md`" in collect_prompt
    assert "AGENTS_PROJECT.md" in collect_prompt
    assert "AI_ARCHITECT_GUIDE.md" in collect_prompt
    assert "<document>" not in collect_prompt
    assert "Build one reusable collector context" not in collect_prompt
    assert "## Force-Injected Documents" in plan_prompt
    assert "`/root/squadder-devops/docs/AGENTS.md`" in plan_prompt
    assert "AGENTS_PROJECT.md" in plan_prompt
    assert "AI_ARCHITECT_GUIDE.md" in plan_prompt
    assert "task_template.md" in plan_prompt
    assert "task_artifact_writes" in plan_prompt
    assert "## Dynamic Document Candidates" not in plan_prompt
    assert "## Force-Injected Documents" in review_prompt
    assert "## Checklist Guardrail Items" in review_prompt
    assert "## Dynamic Document Candidates" not in review_prompt
    assert "AGENTS_PROJECT.md" in review_prompt
    assert "DEVOPS_GUIDE.md" in review_prompt
    assert "code_semantic_markup.md" in review_prompt
    assert "checklist_resolutions" in review_prompt
    assert "- executor_payload:" in review_prompt
    assert "summary: square.py updated" in review_prompt


def test_human_gate_roundtrip(initial_state):
    scripted_driver = ScriptedDriver(
        {
            ("validate", "executor", "supervisor"): [
                DriverResult(
                    status="ASK_HUMAN",
                    payload={"status": "ASK_HUMAN", "warnings": ["Need approval before finalize"]},
                    raw_text="```yaml\nstatus: ASK_HUMAN\nwarnings:\n  - Need approval before finalize\n```",
                ),
                DriverResult(
                    status="PASS",
                    payload={
                        "status": "PASS",
                        "cross_cutting_result": "PASS",
                        "final_result": "Approved by human and validated",
                        "warnings": [],
                    },
                    raw_text="```yaml\nstatus: PASS\ncross_cutting_result: PASS\nfinal_result: Approved by human and validated\nwarnings: []\n```",
                ),
            ]
        }
    )

    app = compile_graph(driver=scripted_driver, checkpointer=MemorySaver())
    config = {"configurable": {"thread_id": "human-gate-thread"}}

    interrupted = app.invoke(initial_state, config)
    assert "__interrupt__" in interrupted

    resumed = app.invoke(Command(resume={"approved": True}), config)
    assert resumed["current_status"] == "PASS"
    assert resumed["final_result"] == "Approved by human and validated"
    assert resumed["human_decisions"][-1]["response"] == {"approved": True}
    assert resumed["pending_approval_ref"] is None
    assert resumed["human_decision_refs"]


def test_execute_replan_loop_escalates_to_human_gate_after_executor_budget(task_artifacts):
    scripted_driver = ScriptedDriver(
        {
            ("plan", "executor", "supervisor"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "plan": [
                            {
                                "id": "backend-create-square-file",
                                "role": "backend",
                                "description": "Create square.py",
                                "dependencies": [],
                                "max_retries": 3,
                            }
                        ],
                        "warnings": [],
                    },
                    raw_text="mock-plan-pass",
                )
            ],
            ("execute", "executor", "backend"): [
                DriverResult(
                    status=PipelineStatus.NEEDS_FIX_EXECUTOR,
                    payload={
                        "status": PipelineStatus.NEEDS_FIX_EXECUTOR,
                        "warnings": ["Executor returned non-YAML final output"],
                    },
                    raw_text="plain text finish",
                ),
                DriverResult(
                    status=PipelineStatus.NEEDS_FIX_EXECUTOR,
                    payload={
                        "status": PipelineStatus.NEEDS_FIX_EXECUTOR,
                        "warnings": ["Executor returned non-YAML final output"],
                    },
                    raw_text="plain text finish",
                ),
                DriverResult(
                    status=PipelineStatus.NEEDS_FIX_EXECUTOR,
                    payload={
                        "status": PipelineStatus.NEEDS_FIX_EXECUTOR,
                        "warnings": ["Executor returned non-YAML final output"],
                    },
                    raw_text="plain text finish",
                ),
            ],
        }
    )
    state = {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Create square.py",
        "workspace_root": "/root/squadder-devops",
        "task_worktree_root": task_artifacts["task_worktree_root"],
        "primary_workspace_repo_id": "devops",
        "source_workspace_roots": {
            "devops": "/root/squadder-devops",
            "backend-prod": "/root/dev-prod-squadder/app",
        },
        "role_workspace_repo_map": {
            "devops": "devops",
            "architect": "devops",
            "backend": "backend-prod",
        },
        "task_workspace_repos": task_artifacts["task_workspace_repos"],
        "trace_id": "test-graph-execute-budget",
        "task_dir_path": task_artifacts["task_dir_path"],
        "task_card_path": task_artifacts["task_card_path"],
        "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
        "current_state": {},
        "plan": [],
        "structured_outputs": [],
        "human_decisions": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {},
        "runtime_step_refs": [],
        "latest_step_ref_by_key": {},
        "pending_approval_ref": None,
        "human_decision_refs": [],
        "cleanup_manifest_ref": None,
        "commits": [],
    }

    app = compile_graph(driver=scripted_driver, checkpointer=MemorySaver())
    interrupted = app.invoke(state, {"configurable": {"thread_id": "execute-budget-thread"}})

    assert "__interrupt__" in interrupted
    assert scripted_driver.calls[("plan", "executor", "supervisor")] == 1
    assert scripted_driver.calls[("execute", "executor", "backend")] == 3


def test_collect_current_state_is_forwarded_to_plan_executor(initial_state):
    runtime = get_runtime_config()
    collected_snapshot = {
        "methodology_summary": {
            "entrypoint": "/root/squadder-devops/docs/AGENTS.md",
            "task_template": "/root/squadder-devops/docs/common/templates/task_template.md",
        },
        "repo_snapshot": {
            "branch": "main",
            "dirty": True,
        },
    }
    captured_collect_reviewer_context: dict[str, object] = {}
    captured_plan_context: dict[str, object] = {}

    def capture_collect_reviewer_request(request, _call_number):
        captured_collect_reviewer_context.update(request.task_context)
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "feedback": "Snapshot complete",
                "warnings": [],
            },
            raw_text="mock-collect-review-pass",
        )

    def capture_plan_request(request, _call_number):
        captured_plan_context.update(request.task_context)
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "plan": [
                    {
                        "id": "devops-create-file",
                        "role": "devops",
                        "description": "Create file in task worktree",
                        "dependencies": [],
                    }
                ],
                "warnings": [],
            },
            raw_text="mock-plan-pass",
        )

    driver = ScriptedDriver(
        {
            ("collect", "executor", "collector"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "current_state": collected_snapshot,
                        "warnings": [],
                    },
                    raw_text="mock-collect-pass",
                )
            ],
            ("collect", "reviewer", "collector"): [capture_collect_reviewer_request],
            ("plan", "executor", "supervisor"): [capture_plan_request],
            ("plan", "reviewer", "supervisor"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "feedback": "Plan looks good",
                        "warnings": [],
                    },
                    raw_text="mock-plan-review-pass",
                )
            ],
        }
    )
    task_unit_runner = TaskUnitRunner(driver)

    collect_updates = run_collect_phase(
        initial_state,
        task_unit_runner=task_unit_runner,
        phase_config=runtime.phases["collect"],
    )
    state_after_collect = {**initial_state, **collect_updates}

    assert captured_collect_reviewer_context["current_state"] == collected_snapshot
    assert captured_collect_reviewer_context["collector_result_meta"] == {
        "status": "PASS",
        "warnings": [],
    }
    assert sorted(captured_collect_reviewer_context["task_workspace_repos"].keys()) == [
        "backend-prod",
        "devops",
    ]
    assert "executor_payload" not in captured_collect_reviewer_context
    assert state_after_collect["current_state"] == collected_snapshot

    run_plan_phase(
        state_after_collect,
        task_unit_runner=task_unit_runner,
        phase_config=runtime.phases["plan"],
    )

    assert captured_plan_context["current_state"] == collected_snapshot
    assert captured_plan_context["task_workspace_repos"] == initial_state["task_workspace_repos"]


def test_collect_phase_omits_missing_parent_task_card_from_task_context(tmp_path):
    runtime = get_runtime_config()
    task_dir = tmp_path / "task-dir"
    task_dir.mkdir(parents=True, exist_ok=True)
    captured_collect_executor_context: dict[str, object] = {}

    def capture_collect_executor_request(request, _call_number):
        captured_collect_executor_context.update(request.task_context)
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "current_state": {"repo_snapshot": {"dirty": False}},
                "warnings": [],
            },
            raw_text="mock-collect-pass",
        )

    driver = ScriptedDriver(
        {
            ("collect", "executor", "collector"): [capture_collect_executor_request],
            ("collect", "reviewer", "collector"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "feedback": "Collect output is valid",
                        "warnings": [],
                    },
                    raw_text="mock-collect-review-pass",
                )
            ],
        }
    )
    state = {
        "task_id": "2026-04-06_1900__missing-parent-task-card",
        "user_request": "Collect repo context only",
        "workspace_root": str(tmp_path),
        "task_worktree_root": str(task_dir / "workspace"),
        "primary_workspace_repo_id": "devops",
        "source_workspace_roots": {"devops": str(tmp_path / "source-devops")},
        "role_workspace_repo_map": {"devops": "devops"},
        "task_workspace_repos": {"devops": str(task_dir / "workspace" / "devops")},
        "trace_id": "test-collect-no-parent-task-card",
        "task_dir_path": str(task_dir),
        "task_card_path": str(task_dir / "TASK.md"),
        "openhands_conversations_dir": str(task_dir / "runtime_artifacts" / "openhands_conversations"),
        "current_state": {},
        "plan": [],
        "structured_outputs": [],
        "human_decisions": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {},
        "runtime_step_refs": [],
        "latest_step_ref_by_key": {},
        "pending_approval_ref": None,
        "human_decision_refs": [],
        "cleanup_manifest_ref": None,
        "commits": [],
    }
    task_unit_runner = TaskUnitRunner(driver)

    run_collect_phase(
        state,
        task_unit_runner=task_unit_runner,
        phase_config=runtime.phases["collect"],
    )

    assert "task_card_path" not in captured_collect_executor_context


def test_task_unit_executor_retry_receives_guardrail_feedback(task_artifacts):
    runtime = get_runtime_config()
    execute_pipeline = runtime.phases["execute"].default_worker_pipeline
    captured_retry_context: dict[str, object] = {}

    def second_executor_attempt(request, _call_number):
        captured_retry_context.update(request.task_context)
        assert request.metadata["reuse_conversation_id"] == "conv-first"
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "structured_output": {
                    "task_id": "task-1",
                    "subtask_id": "backend-create-square-file",
                    "role": "backend",
                    "status": "done",
                    "changes": [],
                    "commands_executed": [],
                    "tests_passed": [],
                    "commits": [],
                    "warnings": [],
                    "summary": "retry fixed guardrail failure",
                },
                "warnings": [],
            },
            raw_text="second-attempt",
            conversation_id="conv-first",
        )

    scripted_driver = ScriptedDriver(
        {
            ("execute", "executor", "backend"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "structured_output": {
                            "task_id": "task-1",
                            "subtask_id": "backend-create-square-file",
                            "role": "backend",
                            "status": "done",
                            "changes": [],
                            "commands_executed": [],
                            "tests_passed": [],
                            "commits": [],
                            "warnings": [],
                        },
                        "warnings": [],
                    },
                    raw_text="first-attempt",
                    conversation_id="conv-first",
                ),
                second_executor_attempt,
            ],
            ("execute", "reviewer", "backend"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "feedback": "looks good",
                        "issues": [],
                        "warnings": [],
                    },
                    raw_text="review-pass",
                )
            ],
            ("execute", "tester", "backend"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "result": {
                            "tests": [{"name": "smoke", "status": "pass", "output": "ok"}],
                            "diagnostics": None,
                        },
                        "feedback": "tests passed",
                        "warnings": [],
                    },
                    raw_text="tester-pass",
                )
            ],
        }
    )

    result = TaskUnitRunner(scripted_driver).run(
        phase_id=PhaseId.EXECUTE,
        role_dir="backend",
        pipeline=execute_pipeline,
        task_context={
            "task_id": "task-1",
            "subtask_id": "backend-create-square-file",
            "subtask_description": "Create square.py",
            "task_dir_path": task_artifacts["task_dir_path"],
            "task_card_path": task_artifacts["task_card_path"],
            "subtask_card_path": task_artifacts["subtask_card_path"],
            "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
            "task_worktree_root": task_artifacts["task_worktree_root"],
        },
        working_dir=task_artifacts["task_workspace_repos"]["backend-prod"],
        metadata={"trace_id": "test-task-unit-guardrail-retry"},
        trace_id="test-task-unit-guardrail-retry",
    )

    assert result.status == PipelineStatus.PASS
    assert result.executor_attempts_used == 2
    assert scripted_driver.calls[("execute", "executor", "backend")] == 2
    assert "previous_guardrail_failures" in captured_retry_context
    assert captured_retry_context["previous_guardrail_failures"]
    assert "latest_guardrail_feedback" in captured_retry_context
    assert "structured_output missing key: summary" in str(
        captured_retry_context["latest_guardrail_feedback"]
    )
    assert captured_retry_context["previous_feedback"] == captured_retry_context["latest_guardrail_feedback"]


def test_task_unit_executor_drops_reuse_after_runtime_error(task_artifacts):
    runtime = get_runtime_config()
    execute_pipeline = runtime.phases["execute"].default_worker_pipeline
    captured_third_attempt_metadata: dict[str, object] = {}

    def third_executor_attempt(request, _call_number):
        captured_third_attempt_metadata.update(request.metadata)
        assert "reuse_conversation_id" not in request.metadata
        return DriverResult(
            status=PipelineStatus.PASS,
            payload={
                "status": PipelineStatus.PASS,
                "structured_output": {
                    "task_id": "task-1",
                    "subtask_id": "backend-create-square-file",
                    "role": "backend",
                    "status": "done",
                    "changes": [],
                    "commands_executed": [],
                    "tests_passed": [],
                    "commits": [],
                    "warnings": [],
                    "summary": "fresh conversation recovered after runtime error",
                },
                "checklist_resolutions": [],
                "warnings": [],
            },
            raw_text="third-attempt",
            conversation_id="conv-fresh",
        )

    scripted_driver = ScriptedDriver(
        {
            ("execute", "executor", "backend"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "structured_output": {
                            "task_id": "task-1",
                            "subtask_id": "backend-create-square-file",
                            "role": "backend",
                            "status": "done",
                            "changes": [],
                            "commands_executed": [],
                            "tests_passed": [],
                            "commits": [],
                            "warnings": [],
                        },
                        "warnings": [],
                    },
                    raw_text="first-attempt",
                    conversation_id="conv-first",
                ),
                DriverResult(
                    status=PipelineStatus.NEEDS_FIX_EXECUTOR,
                    payload={
                        "status": PipelineStatus.NEEDS_FIX_EXECUTOR,
                        "warnings": ["OpenHands execution_status=error"],
                    },
                    raw_text="second-attempt",
                    conversation_id="conv-first",
                ),
                third_executor_attempt,
            ],
            ("execute", "reviewer", "backend"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "feedback": "looks good",
                        "issues": [],
                        "warnings": [],
                    },
                    raw_text="review-pass",
                )
            ],
            ("execute", "tester", "backend"): [
                DriverResult(
                    status=PipelineStatus.PASS,
                    payload={
                        "status": PipelineStatus.PASS,
                        "result": {
                            "tests": [{"name": "smoke", "status": "pass", "output": "ok"}],
                            "diagnostics": None,
                        },
                        "feedback": "tests passed",
                        "warnings": [],
                    },
                    raw_text="tester-pass",
                )
            ],
        }
    )

    result = TaskUnitRunner(scripted_driver).run(
        phase_id=PhaseId.EXECUTE,
        role_dir="backend",
        pipeline=execute_pipeline,
        task_context={
            "task_id": "task-1",
            "subtask_id": "backend-create-square-file",
            "subtask_description": "Create square.py",
            "task_dir_path": task_artifacts["task_dir_path"],
            "task_card_path": task_artifacts["task_card_path"],
            "subtask_card_path": task_artifacts["subtask_card_path"],
            "openhands_conversations_dir": task_artifacts["openhands_conversations_dir"],
            "task_worktree_root": task_artifacts["task_worktree_root"],
        },
        working_dir=task_artifacts["task_workspace_repos"]["backend-prod"],
        metadata={"trace_id": "test-task-unit-reset-broken-reuse"},
        trace_id="test-task-unit-reset-broken-reuse",
    )

    assert result.status == PipelineStatus.ESCALATE_TO_HUMAN
    assert result.executor_attempts_used == 3
    assert scripted_driver.calls[("execute", "executor", "backend")] == 3
    assert captured_third_attempt_metadata["attempt"] == 3
