"""V1 graph compiler package."""

from squadder_orchestrator.graph_compiler.state_schema import PipelineState, StructuredOutput, SubtaskState


def compile_graph(*args, **kwargs):
    from squadder_orchestrator.graph_compiler.langgraph_builder import compile_graph as _compile_graph

    return _compile_graph(*args, **kwargs)


__all__ = ["compile_graph", "PipelineState", "StructuredOutput", "SubtaskState"]
