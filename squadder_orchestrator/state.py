"""Compatibility re-exports for the V1 phase-driven state schema."""

from squadder_orchestrator.graph_compiler.state_schema import (
    FileChange,
    PipelineState,
    StructuredOutput,
    SubtaskState,
    TaskUnitResult,
)


AgentState = PipelineState
Subtask = SubtaskState

__all__ = [
    "AgentState",
    "FileChange",
    "PipelineState",
    "StructuredOutput",
    "Subtask",
    "SubtaskState",
    "TaskUnitResult",
]
