"""V1 phase-driven flow tests."""

from __future__ import annotations

from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from workflow_runtime.agent_drivers.base_driver import DriverResult
from workflow_runtime.agent_drivers.mock_driver import MockDriver
from workflow_runtime.graph_compiler.langgraph_builder import _build_driver, compile_graph
from workflow_runtime.graph_compiler.state_schema import PhaseId, PipelineStatus
from workflow_runtime.integrations.phase_config_loader import (
    get_flow_manifest,
    get_runtime_config,
    load_all_role_metadata,
)
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
    assert runtime.phases["execute"].strategy.max_concurrent == 1
    assert runtime.methodology_root_default == "/root/squadder-devops/docs"
    assert runtime.methodology_agents_entrypoint == "AGENTS.md"
    assert runtime.openhands["base_url_default"] == "http://127.0.0.1:8011"
    assert runtime.openhands["methodology_root_runtime"] == "/root/squadder-devops/docs"
    assert runtime.openhands["max_poll_interval_seconds"] == 15
    assert runtime.openhands["poll_log_every_n_attempts"] == 5
    assert flow.version == "1.0"
    assert flow.start_phase == "collect"


def test_build_driver_uses_runtime_openhands_base_url_default(monkeypatch):
    runtime = get_runtime_config()
    monkeypatch.delenv("OPENHANDS_BASE_URL", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    driver = _build_driver("openhands", runtime)

    assert driver._api._base_url == "http://127.0.0.1:8011"


def test_prompt_builder_loads_role_prompt_and_runtime_sections():
    runtime = get_runtime_config()
    prompt = compose_prompt(
        phase_id=PhaseId.EXECUTE,
        role_dir="devops",
        step_config=runtime.phases["execute"].default_worker_pipeline.executor,
        task_context={},
    )
    assert "# Role: devops / executor" in prompt
    assert "executor_common.md" in prompt
    assert "Methodology Bootstrap" in prompt
    assert "`/root/squadder-devops/docs/AGENTS.md`" in prompt
    assert "Runtime Task Context" in prompt
    assert "Output Contract" in prompt


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
        "commits": [],
    }

    app = compile_graph(driver=scripted_driver, checkpointer=MemorySaver())
    interrupted = app.invoke(state, {"configurable": {"thread_id": "execute-budget-thread"}})

    assert "__interrupt__" in interrupted
    assert scripted_driver.calls[("plan", "executor", "supervisor")] == 1
    assert scripted_driver.calls[("execute", "executor", "backend")] == 3
