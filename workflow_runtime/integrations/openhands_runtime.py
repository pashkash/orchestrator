"""Shared OpenHands runtime constants and execution-status helpers."""

from __future__ import annotations

from enum import StrEnum

OPENHANDS_EVENT_SEARCH_LIMIT_MAX = 100
OPENHANDS_REQUIRED_TOOL_NAMES = (
    "terminal",
    "file_editor",
)


class OpenHandsExecutionStatus(StrEnum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    COMPLETED = "COMPLETED"
    DONE = "DONE"
    PAUSED = "PAUSED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


OPENHANDS_TERMINAL_EXECUTION_STATUSES = frozenset(
    {
        OpenHandsExecutionStatus.FINISHED,
        OpenHandsExecutionStatus.COMPLETED,
        OpenHandsExecutionStatus.DONE,
        OpenHandsExecutionStatus.PAUSED,
        OpenHandsExecutionStatus.FAILED,
        OpenHandsExecutionStatus.ERROR,
        OpenHandsExecutionStatus.CANCELLED,
    }
)


def normalize_openhands_execution_status(raw_status: str | None) -> OpenHandsExecutionStatus | None:
    normalized = str(raw_status or "").strip().upper()
    if not normalized:
        return None
    try:
        return OpenHandsExecutionStatus(normalized)
    except ValueError:
        return None
