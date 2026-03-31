"""Shared pytest fixtures for the V1 orchestrator."""

from __future__ import annotations

import pytest


@pytest.fixture
def initial_state() -> dict:
    return {
        "task_id": "2026-03-24_1800__multi-agent-system-design",
        "user_request": "Refactor orchestrator to the V1 phase-driven architecture",
        "workspace_root": "/root/squadder-devops",
        "trace_id": "test-trace-id",
        "current_state": {},
        "plan": [],
        "structured_outputs": [],
        "human_decisions": [],
        "execution_errors": [],
        "phase_outputs": {},
        "phase_attempts": {},
        "commits": [],
    }
