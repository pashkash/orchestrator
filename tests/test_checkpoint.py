"""Checkpoint persistence tests for the V1 orchestrator."""

from __future__ import annotations

import sqlite3

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

from workflow_runtime.agent_drivers.base_driver import DriverResult
from workflow_runtime.agent_drivers.mock_driver import MockDriver
from workflow_runtime.graph_compiler.langgraph_builder import compile_graph
from tests.mocks import ScriptedDriver


def test_sqlite_checkpoint_saves_final_state(tmp_path, initial_state):
    db_path = str(tmp_path / "checkpoints.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    checkpointer = SqliteSaver(conn)
    checkpointer.setup()

    app = compile_graph(driver=MockDriver(), checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "sqlite-final-state"}}

    result = app.invoke(initial_state, config)
    snapshot = app.get_state(config)

    assert result["final_result"] == "Mock validation succeeded"
    assert snapshot.values["final_result"] == "Mock validation succeeded"
    conn.close()


def test_sqlite_checkpoint_resume_after_human_gate(tmp_path, initial_state):
    db_path = str(tmp_path / "checkpoints.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    checkpointer = SqliteSaver(conn)
    checkpointer.setup()

    scripted_driver = ScriptedDriver(
        {
            ("validate", "executor", "supervisor"): [
                DriverResult(
                    status="ASK_HUMAN",
                    payload={"status": "ASK_HUMAN", "warnings": ["Need human review"]},
                    raw_text="```yaml\nstatus: ASK_HUMAN\nwarnings:\n  - Need human review\n```",
                ),
                DriverResult(
                    status="PASS",
                    payload={
                        "status": "PASS",
                        "cross_cutting_result": "PASS",
                        "final_result": "Resumed from SQLite checkpoint",
                        "warnings": [],
                    },
                    raw_text="```yaml\nstatus: PASS\ncross_cutting_result: PASS\nfinal_result: Resumed from SQLite checkpoint\nwarnings: []\n```",
                ),
            ]
        }
    )

    app = compile_graph(driver=scripted_driver, checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "sqlite-human-gate"}}

    interrupted = app.invoke(initial_state, config)
    assert "__interrupt__" in interrupted

    snapshots = list(app.get_state_history(config))
    assert snapshots

    resumed = app.invoke(Command(resume={"approved": True}), config)
    assert resumed["final_result"] == "Resumed from SQLite checkpoint"
    assert resumed["human_decision_refs"]
    conn.close()
