"""V1 phase-driven flow tests."""

from __future__ import annotations

from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from squadder_orchestrator.agent_drivers.base_driver import DriverResult
from squadder_orchestrator.agent_drivers.mock_driver import MockDriver
from squadder_orchestrator.config import build_prompt, load_all_roles, load_flow, load_runtime
from squadder_orchestrator.graph import compile_graph
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
    flow = load_flow()
    app = compile_graph(driver=MockDriver())

    graph_nodes = {name for name in app.get_graph().nodes.keys() if not name.startswith("__")}
    manifest_nodes = {phase.id for phase in flow.phases}
    assert graph_nodes == manifest_nodes


def test_runtime_loaders_reflect_v1_defaults():
    roles = load_all_roles()
    runtime = load_runtime()
    flow = load_flow()

    assert "devops" in roles
    assert "collector" in roles
    assert runtime.phases["execute"].strategy.max_concurrent == 1
    assert flow.version == "1.0"
    assert flow.start_phase == "collect"


def test_prompt_builder_reads_shared_and_runtime_contract():
    prompt = build_prompt("devops", "executor")
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
